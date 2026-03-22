"""
=============================================================================
PROCESAMIENTO BATCH EC2 — PubMed Baseline (~1,274 archivos XML)
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Versión optimizada para EC2 Ubuntu (2 vCPUs, ~911 MB RAM)

Diferencias respecto a la versión anterior (batch_processor.py):
  - Descarga y descomprime en MEMORIA (no guarda .gz ni .xml en disco)
  - Solo escribe el JSONL final — ahorra ~420 GB de espacio en disco
  - 1 worker secuencial — adaptado a la RAM disponible en EC2
  - Elimina abstract_sections del registro (decisión de diseño del proyecto)
  - Rutas Linux (/home/ubuntu/...) en lugar de rutas Windows

Tiempo estimado: 14–17 horas para los ~1,274 archivos del baseline

Uso en EC2:
    source ~/pubmed_env/bin/activate
    screen -S pubmed
    python3 batch_processor_ec2.py
    # Ctrl+A, D → desconectarte (proceso sigue corriendo)
    # screen -r pubmed → reconectarte cuando quiera revisar
=============================================================================
"""

import os
import re
import json
import gzip
import time
import logging
import fnmatch
import requests
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.parse import urljoin

from tqdm import tqdm

from pubmed_pipeline import extraer_articulos_de_xml_desde_bytes, guardar_jsonl

# ─────────────────────────────────────────────────────────────────────────────
# ★ CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # URL oficial del baseline de PubMed
    "url_baseline": "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",

    # Carpeta de salida en EC2 (solo se escribe el JSONL — no se guardan XMLs)
    "output_dir": "/home/ubuntu/pubmed/corpus",

    # Patrón de archivos a descargar
    "patron_archivos": "pubmed26n*.xml",

    # None = descargar todos (~1,274 archivos)
    # Número entero = limitar para pruebas, ej: 5
    "max_descargas": 3,

    # Segundos de espera entre descargas (respeto al servidor del NIH)
    "delay_segundos": 1,

    # Máximo de reintentos por archivo si falla la descarga
    "max_reintentos": 5,

    # Timeout de descarga en segundos
    "timeout": 120,
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
    """
    Guarda qué archivos ya fueron procesados exitosamente.
    Si el script se interrumpe, al reiniciar salta los ya procesados.
    """

    def __init__(self, output_dir: str):
        self.ruta = os.path.join(output_dir, "checkpoint.json")
        self.procesados = self._cargar()

    def _cargar(self) -> set:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r", encoding="utf-8") as f:
                data = json.load(f)
            total = len(data.get("procesados", []))
            print(f"  ✓ Checkpoint encontrado: {total:,} archivos ya procesados")
            return set(data["procesados"])
        return set()

    def marcar_lote(self, nombres: list):
        self.procesados.update(nombres)
        self._guardar()

    def _guardar(self):
        with open(self.ruta, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "procesados": list(self.procesados),
                    "ultima_actualizacion": datetime.now().isoformat(),
                },
                f, indent=2,
            )

    def pendientes(self, todos: list) -> list:
        return [f for f in todos if f not in self.procesados]


# ─────────────────────────────────────────────────────────────────────────────
# LISTAR ARCHIVOS REMOTOS
# ─────────────────────────────────────────────────────────────────────────────

def listar_archivos_remotos(url_baseline: str, patron: str) -> list:
    """
    Consulta el índice HTTP del FTP de PubMed y devuelve los nombres
    de archivos .gz que coinciden con el patrón.
    """
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
# DESCARGA Y DESCOMPRESIÓN EN MEMORIA
# ─────────────────────────────────────────────────────────────────────────────

def descargar_y_descomprimir(url: str, max_reintentos: int, timeout: int) -> bytes | None:
    """
    Descarga un archivo .xml.gz y lo descomprime EN MEMORIA.
    No guarda ningún archivo en disco — ahorra ~420 GB de espacio.

    Retorna los bytes del XML descomprimido, o None si falló.
    """
    for intento in range(1, max_reintentos + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=False)

            if resp.status_code == 404:
                return None  # Archivo no existe en el servidor

            resp.raise_for_status()

            # Descomprimir en memoria sin tocar el disco
            with gzip.open(BytesIO(resp.content), "rb") as f:
                xml_bytes = f.read()

            return xml_bytes

        except requests.exceptions.Timeout:
            log_msg = f"Timeout (intento {intento}/{max_reintentos})"
        except requests.exceptions.ConnectionError:
            log_msg = f"Error de conexión (intento {intento}/{max_reintentos})"
        except gzip.BadGzipFile:
            log_msg = f"Archivo .gz corrupto (intento {intento}/{max_reintentos})"
        except Exception as e:
            log_msg = f"Error inesperado: {e} (intento {intento}/{max_reintentos})"

        if intento < max_reintentos:
            espera = 2 ** intento  # Espera exponencial: 2s, 4s, 8s, 16s
            time.sleep(espera)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_batch(config: dict):
    """
    Pipeline secuencial optimizado para EC2:
      1. Lista archivos disponibles en el FTP de PubMed
      2. Filtra los ya procesados (checkpoint)
      3. Por cada archivo: descarga → descomprime en RAM → extrae → guarda JSONL
      4. Checkpoint automático cada 10 archivos
      5. Reporte final con estadísticas
    """
    output_dir   = config["output_dir"]
    url_baseline = config["url_baseline"]
    if not url_baseline.endswith("/"):
        url_baseline += "/"

    os.makedirs(output_dir, exist_ok=True)
    log = configurar_logging(output_dir)
    checkpoint = Checkpoint(output_dir)

    ruta_jsonl  = os.path.join(output_dir, "pubmed_corpus.jsonl")
    ruta_reporte = os.path.join(output_dir, "reporte_batch.json")

    # ── Listar archivos remotos ──────────────────────────────────────────────
    log.info("Consultando índice del FTP de PubMed...")
    remotos = listar_archivos_remotos(url_baseline, config["patron_archivos"])

    if not remotos:
        log.error(f"No se encontraron archivos con patrón: {config['patron_archivos']}")
        return

    # Limitar si max_descargas está configurado (útil para pruebas)
    if config.get("max_descargas"):
        remotos = remotos[:config["max_descargas"]]

    # ── Filtrar pendientes ───────────────────────────────────────────────────
    pendientes = checkpoint.pendientes(remotos)

    log.info(f"\n{'='*60}")
    log.info(f"PROCESAMIENTO BATCH EC2 — PubMed Baseline")
    log.info(f"{'='*60}")
    log.info(f"  Total archivos en FTP     : {len(remotos):,}")
    log.info(f"  Ya procesados (checkpoint): {len(remotos) - len(pendientes):,}")
    log.info(f"  Pendientes                : {len(pendientes):,}")
    log.info(f"  Estrategia                : descarga + descompresión EN MEMORIA")
    log.info(f"  Espacio en disco necesario: solo para el JSONL (~50-80 GB)")
    log.info(f"  Tiempo estimado           : {len(pendientes) * 45 / 3600:.1f} horas aprox.")
    log.info(f"  Corpus de salida          : {ruta_jsonl}")
    log.info(f"{'='*60}\n")

    if not pendientes:
        log.info("✓ Todos los archivos ya fueron procesados.")
        return

    # ── Verificar espacio en disco antes de empezar ──────────────────────────
    statvfs = os.statvfs(output_dir)
    gb_libres = (statvfs.f_frsize * statvfs.f_bavail) / (1024 ** 3)
    log.info(f"  Espacio libre en disco: {gb_libres:.1f} GB")
    if gb_libres < 10:
        log.error(f"  ✗ Espacio insuficiente ({gb_libres:.1f} GB). Se necesitan al menos 10 GB.")
        return

    # ── Procesamiento secuencial ─────────────────────────────────────────────
    total_registros  = 0
    archivos_ok      = 0
    archivos_fallidos = []
    nombres_lote     = []
    inicio_total     = time.time()

    with tqdm(
        total=len(pendientes),
        desc="Procesando",
        unit="archivo",
        ncols=80,
        colour="green",
    ) as barra:

        for nombre_gz in pendientes:
            url = urljoin(url_baseline, nombre_gz)
            nombre_xml = nombre_gz.replace(".gz", "")
            inicio_archivo = time.time()

            barra.set_postfix({
                "archivo":   nombre_gz[-18:],
                "registros": f"{total_registros:,}",
            })

            # ── Paso 1: Descargar y descomprimir en memoria ──────────────────
            xml_bytes = descargar_y_descomprimir(
                url,
                config["max_reintentos"],
                config["timeout"],
            )

            if xml_bytes is None:
                log.warning(f"  ✗ No se pudo descargar: {nombre_gz}")
                archivos_fallidos.append(nombre_gz)
                barra.update(1)
                continue

            # ── Paso 2: Extraer artículos del XML en memoria ─────────────────
            try:
                articulos = extraer_articulos_de_xml_desde_bytes(xml_bytes)
            except Exception as e:
                log.error(f"  ✗ Error parseando {nombre_gz}: {e}")
                archivos_fallidos.append(nombre_gz)
                barra.update(1)
                continue

            # Liberar memoria del XML inmediatamente
            del xml_bytes

            # ── Paso 3: Guardar en JSONL ─────────────────────────────────────
            if articulos:
                guardar_jsonl(articulos, ruta_jsonl, modo="a")
                total_registros += len(articulos)
                archivos_ok += 1

            # Liberar memoria de los artículos
            del articulos

            # ── Paso 4: Checkpoint cada 10 archivos ──────────────────────────
            nombres_lote.append(nombre_gz)
            if len(nombres_lote) >= 10:
                checkpoint.marcar_lote(nombres_lote)
                nombres_lote = []

            # ── Actualizar barra ─────────────────────────────────────────────
            tiempo_archivo = round(time.time() - inicio_archivo, 1)
            barra.set_postfix({
                "archivo":   nombre_xml[-15:],
                "registros": f"{total_registros:,}",
                "seg":       tiempo_archivo,
            })
            barra.update(1)

            # ── Delay entre descargas ────────────────────────────────────────
            time.sleep(config["delay_segundos"])

    # Guardar los últimos en checkpoint
    if nombres_lote:
        checkpoint.marcar_lote(nombres_lote)

    # ── Reporte final ────────────────────────────────────────────────────────
    tiempo_total = time.time() - inicio_total

    # Verificar espacio final
    statvfs = os.statvfs(output_dir)
    gb_libres_final = (statvfs.f_frsize * statvfs.f_bavail) / (1024 ** 3)

    # Tamaño del JSONL generado
    jsonl_gb = os.path.getsize(ruta_jsonl) / (1024 ** 3) if os.path.exists(ruta_jsonl) else 0

    reporte = {
        "resumen": {
            "total_archivos_procesados": archivos_ok + len(archivos_fallidos),
            "exitosos":                  archivos_ok,
            "fallidos":                  len(archivos_fallidos),
            "total_registros_extraidos": total_registros,
            "tiempo_total_horas":        round(tiempo_total / 3600, 2),
            "promedio_seg_por_archivo":  round(tiempo_total / max(archivos_ok, 1), 1),
            "jsonl_tamano_gb":           round(jsonl_gb, 2),
            "espacio_libre_gb":          round(gb_libres_final, 1),
            "fecha_procesamiento":       datetime.now().isoformat(),
        },
        "archivos_fallidos": archivos_fallidos,
    }

    with open(ruta_reporte, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)

    log.info(f"\n{'='*60}")
    log.info(f"PROCESAMIENTO COMPLETADO")
    log.info(f"{'='*60}")
    log.info(f"  Archivos procesados  : {archivos_ok:,}")
    log.info(f"  Archivos fallidos    : {len(archivos_fallidos):,}")
    log.info(f"  Registros extraídos  : {total_registros:,}")
    log.info(f"  Tiempo total         : {str(timedelta(seconds=int(tiempo_total)))}")
    log.info(f"  Promedio por archivo : {reporte['resumen']['promedio_seg_por_archivo']}s")
    log.info(f"  Tamaño del JSONL     : {jsonl_gb:.2f} GB")
    log.info(f"  Espacio libre        : {gb_libres_final:.1f} GB")
    log.info(f"\n  Archivos de salida:")
    log.info(f"    Corpus  → {ruta_jsonl}")
    log.info(f"    Reporte → {ruta_reporte}")
    log.info(f"    Log     → {output_dir}/procesamiento.log")

    if archivos_fallidos:
        log.warning(f"\n  ⚠ {len(archivos_fallidos)} archivos fallaron:")
        for f in archivos_fallidos:
            log.warning(f"    {f}")
        log.warning(f"  Vuelve a ejecutar el script — el checkpoint")
        log.warning(f"  saltará los exitosos y reintentará los fallidos.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("""
╔══════════════════════════════════════════════════════════╗
║   PIPELINE BATCH EC2 — PubMed Baseline                   ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝

  Optimizado para EC2 Ubuntu (2 vCPUs, ~911 MB RAM)
  Estrategia: descarga + descompresión EN MEMORIA
  No guarda archivos .gz ni .xml en disco.
    """)

    print(f"  URL FTP    : {CONFIG['url_baseline']}")
    print(f"  output_dir : {CONFIG['output_dir']}")
    print(f"  max_arch   : {CONFIG['max_descargas'] or 'todos (~1,274)'}")
    print()
    print("  RECUERDA: ejecutar dentro de screen para que no")
    print("  se interrumpa si cierras la consola SSH.")
    print()

    try:
        input("  Presiona ENTER para iniciar (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)

    ejecutar_batch(CONFIG)
