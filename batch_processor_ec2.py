"""
=============================================================================
PROCESAMIENTO BATCH EC2 — PubMed Baseline (~1,274 archivos XML) v3
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Estrategia optimizada para EC2 con RAM limitada (~911 MB):

  DESCARGA A DISCO (no en memoria):
    1. Descarga el .gz a disco por bloques de 1 MB (nunca >1 MB en RAM)
    2. Descomprime .gz → .xml en disco por bloques
    3. Parsea el .xml y guarda un .jsonl individual
    4. Borra .gz y .xml inmediatamente
    → RAM usada: ~50 MB máximo

  UN JSONL POR ARCHIVO XML:
    - pubmed26n0001.jsonl, pubmed26n0002.jsonl, ...
    - Cada archivo ~50-80 MB, abrible en VS Code
    - Compatible con Pyserini (acepta carpeta con múltiples jsonl)
    - Si falla un archivo, el resto no se ve afectado

Tiempo estimado: 14-17 horas para ~1,274 archivos

Uso en EC2:
    source ~/pubmed_env/bin/activate
    screen -S pubmed
    cd ~/Pubmed_Corpus_2026
    python3 batch_processor_ec2.py
    Ctrl+A, D  → desconectarte (proceso sigue corriendo)
    screen -r pubmed → reconectarte
=============================================================================
"""

import os
import re
import json
import gzip
import time
import shutil
import logging
import fnmatch
import requests
from pathlib import Path
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.parse import urljoin

from tqdm import tqdm

from pubmed_pipeline import extraer_articulos_de_xml, guardar_jsonl

# ─────────────────────────────────────────────────────────────────────────────
# ★ CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "url_baseline":    "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
    "temp_dir":        "/home/ubuntu/pubmed/temp",
    "output_dir":      "/home/ubuntu/pubmed/corpus",
    "patron_archivos": "pubmed26n*.xml",
    "max_descargas":   3,        # None = todos | número = prueba
    "delay_segundos":  1,
    "max_reintentos":  5,
    "timeout":         300,
    "chunk_size":      1024 * 1024,  # 1 MB
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def configurar_logging(output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(output_dir, "procesamiento.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────────────────────────────────────

class Checkpoint:
    def __init__(self, output_dir: str):
        self.ruta = os.path.join(output_dir, "checkpoint.json")
        self.procesados = self._cargar()

    def _cargar(self) -> set:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r", encoding="utf-8") as f:
                data = json.load(f)
            total = len(data.get("procesados", []))
            print(f"  ✓ Checkpoint: {total:,} archivos ya procesados")
            return set(data["procesados"])
        return set()

    def marcar(self, nombre: str):
        self.procesados.add(nombre)
        self._guardar()

    def _guardar(self):
        with open(self.ruta, "w", encoding="utf-8") as f:
            json.dump({
                "procesados": list(self.procesados),
                "ultima_actualizacion": datetime.now().isoformat(),
            }, f, indent=2)

    def pendientes(self, todos: list) -> list:
        return [f for f in todos if f not in self.procesados]


# ─────────────────────────────────────────────────────────────────────────────
# LISTAR ARCHIVOS REMOTOS
# ─────────────────────────────────────────────────────────────────────────────

def listar_archivos_remotos(url_baseline: str, patron: str) -> list:
    patron_gz = patron if patron.endswith(".gz") else f"{patron}.gz"
    with urlopen(url_baseline) as response:
        html = response.read().decode("utf-8", errors="ignore")
    archivos = set()
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        nombre = href.split("/")[-1].strip()
        if nombre and fnmatch.fnmatch(nombre, patron_gz):
            archivos.add(nombre)
    return sorted(archivos)


# ─────────────────────────────────────────────────────────────────────────────
# DESCARGA A DISCO POR BLOQUES
# ─────────────────────────────────────────────────────────────────────────────

def descargar_gz(url: str, ruta_gz: str, chunk_size: int,
                 max_reintentos: int, timeout: int, log) -> bool:
    """
    Descarga .gz a disco en bloques de 1 MB.
    Máximo 1 MB en RAM durante la descarga.
    """
    for intento in range(1, max_reintentos + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=True)
            if resp.status_code == 404:
                log.warning(f"  404: {url.split('/')[-1]}")
                return False
            resp.raise_for_status()
            with open(ruta_gz, "wb") as f:
                for bloque in resp.iter_content(chunk_size=chunk_size):
                    if bloque:
                        f.write(bloque)
            return True
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout (intento {intento}/{max_reintentos})")
        except requests.exceptions.ConnectionError:
            log.warning(f"  Error conexión (intento {intento}/{max_reintentos})")
        except Exception as e:
            log.warning(f"  Error: {e} (intento {intento}/{max_reintentos})")
        if os.path.exists(ruta_gz):
            os.remove(ruta_gz)
        if intento < max_reintentos:
            time.sleep(2 ** intento)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# DESCOMPRESIÓN A DISCO POR BLOQUES
# ─────────────────────────────────────────────────────────────────────────────

def descomprimir_gz(ruta_gz: str, ruta_xml: str, log) -> bool:
    """
    Descomprime .gz → .xml en disco por bloques de 1 MB.
    Máximo 1 MB en RAM durante la descompresión.
    """
    try:
        with gzip.open(ruta_gz, "rb") as src, open(ruta_xml, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        return True
    except Exception as e:
        log.error(f"  Error descomprimiendo: {e}")
        if os.path.exists(ruta_xml):
            os.remove(ruta_xml)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# ESPACIO LIBRE EN DISCO
# ─────────────────────────────────────────────────────────────────────────────

def gb_libres(ruta: str) -> float:
    st = os.statvfs(ruta)
    return (st.f_frsize * st.f_bavail) / (1024 ** 3)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_batch(config: dict):
    output_dir   = config["output_dir"]
    temp_dir     = config["temp_dir"]
    url_baseline = config["url_baseline"]
    if not url_baseline.endswith("/"):
        url_baseline += "/"

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    log        = configurar_logging(output_dir)
    checkpoint = Checkpoint(output_dir)

    log.info("Consultando índice del FTP de PubMed...")
    remotos = listar_archivos_remotos(url_baseline, config["patron_archivos"])

    if not remotos:
        log.error("No se encontraron archivos en el FTP.")
        return

    if config.get("max_descargas"):
        remotos = remotos[:config["max_descargas"]]

    pendientes = checkpoint.pendientes(remotos)

    log.info(f"\n{'='*60}")
    log.info(f"PROCESAMIENTO BATCH EC2 — PubMed Baseline v3")
    log.info(f"{'='*60}")
    log.info(f"  Total en FTP    : {len(remotos):,}")
    log.info(f"  Ya procesados   : {len(remotos) - len(pendientes):,}")
    log.info(f"  Pendientes      : {len(pendientes):,}")
    log.info(f"  Estrategia      : descarga a disco + un JSONL por XML")
    log.info(f"  RAM máxima      : ~50 MB (bloques de 1 MB)")
    log.info(f"  Espacio libre   : {gb_libres(output_dir):.1f} GB")
    log.info(f"  Tiempo estimado : {len(pendientes) * 45 / 3600:.1f} horas aprox.")
    log.info(f"  Salida          : {output_dir}/pubmed26nXXXX.jsonl")
    log.info(f"{'='*60}\n")

    if not pendientes:
        log.info("✓ Todos los archivos ya fueron procesados.")
        return

    total_registros   = 0
    archivos_ok       = 0
    archivos_fallidos = []
    inicio_total      = time.time()

    with tqdm(total=len(pendientes), desc="Procesando",
              unit="archivo", ncols=80, colour="green") as barra:

        for nombre_gz in pendientes:

            url      = urljoin(url_baseline, nombre_gz)
            nombre   = nombre_gz.replace(".gz", "")
            stem     = nombre.replace(".xml", "")
            ruta_gz  = os.path.join(temp_dir, nombre_gz)
            ruta_xml = os.path.join(temp_dir, nombre)
            ruta_out = os.path.join(output_dir, f"{stem}.jsonl")

            barra.set_postfix({"archivo": stem[-12:], "total": f"{total_registros:,}"})
            inicio = time.time()

            # Verificar espacio
            if gb_libres(output_dir) < 2:
                log.error("✗ Espacio crítico (<2 GB). Deteniendo.")
                break

            # Paso 1: Descargar .gz
            ok = descargar_gz(url, ruta_gz, config["chunk_size"],
                              config["max_reintentos"], config["timeout"], log)
            if not ok:
                archivos_fallidos.append(nombre_gz)
                barra.update(1)
                continue

            # Paso 2: Descomprimir a .xml
            ok = descomprimir_gz(ruta_gz, ruta_xml, log)
            if os.path.exists(ruta_gz):
                os.remove(ruta_gz)
            if not ok:
                archivos_fallidos.append(nombre_gz)
                barra.update(1)
                continue

            # Paso 3: Parsear XML
            try:
                articulos = extraer_articulos_de_xml(ruta_xml)
            except Exception as e:
                log.error(f"  ✗ Error parseando {nombre}: {e}")
                archivos_fallidos.append(nombre_gz)
                if os.path.exists(ruta_xml):
                    os.remove(ruta_xml)
                barra.update(1)
                continue

            # Borrar XML inmediatamente
            if os.path.exists(ruta_xml):
                os.remove(ruta_xml)

            # Paso 4: Guardar JSONL individual
            if articulos:
                guardar_jsonl(articulos, ruta_out, modo="w")
                total_registros += len(articulos)
                archivos_ok += 1

            checkpoint.marcar(nombre_gz)

            seg = round(time.time() - inicio, 1)
            barra.set_postfix({
                "archivo":   stem[-12:],
                "extraídos": len(articulos) if articulos else 0,
                "total":     f"{total_registros:,}",
                "seg":       seg,
            })
            barra.update(1)
            time.sleep(config["delay_segundos"])

    # Reporte final
    tiempo_total = time.time() - inicio_total
    reporte = {
        "resumen": {
            "exitosos":                  archivos_ok,
            "fallidos":                  len(archivos_fallidos),
            "total_registros_extraidos": total_registros,
            "tiempo_total_horas":        round(tiempo_total / 3600, 2),
            "promedio_seg_por_archivo":  round(tiempo_total / max(archivos_ok, 1), 1),
            "espacio_libre_gb":          round(gb_libres(output_dir), 1),
            "fecha":                     datetime.now().isoformat(),
        },
        "archivos_fallidos": archivos_fallidos,
    }
    with open(os.path.join(output_dir, "reporte_batch.json"), "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)

    log.info(f"\n{'='*60}")
    log.info(f"COMPLETADO")
    log.info(f"  Exitosos          : {archivos_ok:,}")
    log.info(f"  Fallidos          : {len(archivos_fallidos):,}")
    log.info(f"  Registros totales : {total_registros:,}")
    log.info(f"  Tiempo total      : {str(timedelta(seconds=int(tiempo_total)))}")
    log.info(f"  Espacio libre     : {gb_libres(output_dir):.1f} GB")

    if archivos_fallidos:
        log.warning("  ⚠ Vuelve a ejecutar — el checkpoint reintentará los fallidos:")
        for f in archivos_fallidos:
            log.warning(f"    {f}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║   PIPELINE BATCH EC2 — PubMed Baseline  v3               ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝
  Estrategia : descarga a disco + un JSONL por archivo XML
  RAM máxima : ~50 MB (bloques de 1 MB)
    """)
    print(f"  URL FTP    : {CONFIG['url_baseline']}")
    print(f"  temp_dir   : {CONFIG['temp_dir']}")
    print(f"  output_dir : {CONFIG['output_dir']}")
    print(f"  max_arch   : {CONFIG['max_descargas'] or 'todos (~1,274)'}")
    print()
    try:
        input("  Presiona ENTER para iniciar (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)
    ejecutar_batch(CONFIG)
