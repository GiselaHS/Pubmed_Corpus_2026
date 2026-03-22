"""
=============================================================================
PROCESAMIENTO BATCH WEB — PubMed Baseline (~1,200 archivos XML)
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Características:
  ✓ Procesamiento paralelo (usa varios núcleos de tu CPU)
  ✓ Barra de progreso visual en tiempo real
  ✓ Checkpoint: si se interrumpe, al reiniciar continúa donde se quedó
  ✓ Log detallado de errores por archivo
  ✓ Estimación de tiempo restante
  ✓ Reporte final del corpus generado

Uso:
    # Instalar dependencias primero (solo una vez):
    pip install tqdm

    # Ejecutar:
    python batch_processor.py

    # Configurar las rutas en la sección CONFIGURACIÓN abajo
=============================================================================
"""

import os
import json
import time
import logging
import multiprocessing
import fnmatch
import gzip
import re
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from urllib.parse import urljoin
from urllib.request import urlopen

from tqdm import tqdm

# Importa las funciones de extracción del pipeline anterior
# (asegúrate de que pubmed_pipeline.py esté en la misma carpeta)
from pubmed_pipeline import extraer_articulos_de_xml, guardar_jsonl

# ─────────────────────────────────────────────────────────────────────────────
# ★ CONFIGURACIÓN — Edita estas rutas según tu PC
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # URL oficial de PubMed Baseline
    "url_baseline": "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",

    # Carpeta donde se descargan y descomprimen los XMLs
    "download_dir": r"/home/ubuntu/pubmed/temp",

    # Carpeta donde se guardarán los resultados
    "output_dir": r"/home/ubuntu/pubmed/corpus",       # Windows
    # "output_dir": "/home/usuario/pubmed/corpus",  # Linux/Mac

    # Patrón de XMLs finales a procesar (después de descomprimir)
    "patron_archivos": "pubmed26n*.xml",

    # Limita cuántos archivos descargar/procesar (None = todos)
    "max_descargas": 3,

    # Si True, vuelve a descargar y descomprimir aunque ya exista en disco
    "forzar_redescarga": False,

    # Número de núcleos CPU a usar para paralelismo
    # None = detectar automáticamente (usa todos menos 1 para no saturar tu PC)
    "num_workers": None,

    # Tamaño del lote por worker (cuántos XMLs procesa cada worker a la vez)
    "batch_size": 4,
}


def listar_archivos_remotos(url_baseline: str, patron_remoto: str) -> list:
    """Lista nombres de archivo en el índice HTTP del baseline de PubMed."""
    with urlopen(url_baseline) as response:
        html = response.read().decode("utf-8", errors="ignore")

    archivos = set()
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        nombre = href.split("/")[-1].strip()
        if nombre and fnmatch.fnmatch(nombre, patron_remoto):
            archivos.add(nombre)

    return sorted(archivos)


def descargar_archivo(url_archivo: str, ruta_destino: str, forzar: bool = False):
    """Descarga un archivo con barra de progreso."""
    if os.path.exists(ruta_destino) and not forzar:
        return

    os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)

    with urlopen(url_archivo) as response, open(ruta_destino, "wb") as out:
        total = int(response.headers.get("Content-Length") or 0)
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=f"Descargando {Path(ruta_destino).name}",
            leave=False,
            ncols=80,
            colour="cyan",
        ) as barra:
            while True:
                bloque = response.read(1024 * 1024)
                if not bloque:
                    break
                out.write(bloque)
                barra.update(len(bloque))


def descomprimir_gzip(ruta_gz: str, ruta_xml: str, forzar: bool = False):
    """Descomprime un .gz a .xml de forma segura."""
    if os.path.exists(ruta_xml) and not forzar:
        return

    os.makedirs(os.path.dirname(ruta_xml), exist_ok=True)

    with gzip.open(ruta_gz, "rb") as src, open(ruta_xml, "wb") as dst:
        shutil.copyfileobj(src, dst)


def preparar_xmls_desde_web(config: dict, log) -> list:
    """
    Descarga y descomprime XMLs desde PubMed Baseline.

    Retorna una lista de rutas XML locales listas para procesarse.
    """
    url_baseline = config["url_baseline"]
    if not url_baseline.endswith("/"):
        url_baseline += "/"

    patron_xml = config["patron_archivos"]
    patron_remoto = patron_xml if patron_xml.endswith(".gz") else f"{patron_xml}.gz"
    forzar = config.get("forzar_redescarga", False)
    max_descargas = config.get("max_descargas")

    log.info(f"Consultando índice remoto: {url_baseline}")
    remotos = listar_archivos_remotos(url_baseline, patron_remoto)

    if not remotos:
        log.error(f"No se encontraron archivos remotos con patrón: {patron_remoto}")
        return []

    if max_descargas:
        remotos = remotos[:max_descargas]

    download_dir = config["download_dir"]
    os.makedirs(download_dir, exist_ok=True)
    xml_locales = []

    log.info(f"Archivos remotos a preparar: {len(remotos):,}")

    for nombre_gz in remotos:
        url_archivo = urljoin(url_baseline, nombre_gz)
        ruta_gz = os.path.join(download_dir, nombre_gz)
        nombre_xml = nombre_gz[:-3] if nombre_gz.endswith(".gz") else nombre_gz
        ruta_xml = os.path.join(download_dir, nombre_xml)

        log.info(f"Preparando {nombre_gz}")
        descargar_archivo(url_archivo, ruta_gz, forzar=forzar)

        if nombre_gz.endswith(".gz"):
            descomprimir_gzip(ruta_gz, ruta_xml, forzar=forzar)

        xml_locales.append(Path(ruta_xml))

    return xml_locales

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
# CHECKPOINT — Para reanudar si se interrumpe
# ─────────────────────────────────────────────────────────────────────────────

class Checkpoint:
    """
    Guarda qué archivos ya fueron procesados exitosamente.
    Si el script se interrumpe (luz, cierre de laptop, etc.),
    al volver a ejecutar saltará los archivos ya procesados.
    """

    def __init__(self, output_dir: str):
        self.ruta = os.path.join(output_dir, "checkpoint.json")
        self.procesados = self._cargar()

    def _cargar(self) -> set:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"  ✓ Checkpoint encontrado: {len(data['procesados'])} archivos ya procesados")
            return set(data["procesados"])
        return set()

    def marcar(self, nombre_archivo: str):
        self.procesados.add(nombre_archivo)
        self._guardar()

    def marcar_lote(self, nombres: list):
        self.procesados.update(nombres)
        self._guardar()

    def _guardar(self):
        with open(self.ruta, "w", encoding="utf-8") as f:
            json.dump(
                {"procesados": list(self.procesados),
                 "ultima_actualizacion": datetime.now().isoformat()},
                f, indent=2,
            )

    def ya_procesado(self, nombre_archivo: str) -> bool:
        return nombre_archivo in self.procesados

    def pendientes(self, todos: list) -> list:
        return [f for f in todos if Path(f).name not in self.procesados]


# ─────────────────────────────────────────────────────────────────────────────
# WORKER — Función que ejecuta cada proceso paralelo
# ─────────────────────────────────────────────────────────────────────────────

def procesar_un_xml(args) -> dict:
    """
    Procesa un único archivo XML y devuelve el resultado.
    Esta función corre en un proceso separado (worker).

    Retorna un diccionario con:
        - nombre     : nombre del archivo
        - registros  : número de artículos extraídos
        - exitoso    : True/False
        - error      : mensaje de error si falló
        - tiempo_seg : segundos que tardó
    """
    file_path, output_dir = args
    nombre = Path(file_path).name
    inicio = time.time()

    try:
        articulos = extraer_articulos_de_xml(file_path)
        if articulos:
            ruta_salida = os.path.join(output_dir, "pubmed_corpus.jsonl")
            guardar_jsonl(articulos, ruta_salida, modo="a")

        return {
            "nombre":     nombre,
            "registros":  len(articulos),
            "exitoso":    True,
            "error":      None,
            "tiempo_seg": round(time.time() - inicio, 2),
        }

    except Exception as e:
        return {
            "nombre":     nombre,
            "registros":  0,
            "exitoso":    False,
            "error":      str(e),
            "tiempo_seg": round(time.time() - inicio, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# REPORTE FINAL
# ─────────────────────────────────────────────────────────────────────────────

def guardar_reporte(resultados: list, output_dir: str, tiempo_total: float):
    """Genera un reporte JSON con el resumen del procesamiento batch."""

    exitosos  = [r for r in resultados if r["exitoso"]]
    fallidos  = [r for r in resultados if not r["exitoso"]]
    total_reg = sum(r["registros"] for r in exitosos)

    reporte = {
        "resumen": {
            "total_archivos_procesados": len(resultados),
            "exitosos":                  len(exitosos),
            "fallidos":                  len(fallidos),
            "total_registros_extraidos": total_reg,
            "tiempo_total_minutos":      round(tiempo_total / 60, 2),
            "promedio_seg_por_archivo":  round(tiempo_total / max(len(resultados), 1), 2),
            "fecha_procesamiento":       datetime.now().isoformat(),
        },
        "archivos_fallidos": fallidos,
    }

    ruta = os.path.join(output_dir, "reporte_batch.json")
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(reporte, f, ensure_ascii=False, indent=2)

    return reporte


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE BATCH PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_batch(config: dict):
    """
    Orquesta el procesamiento paralelo de todos los XMLs.

    Flujo:
            1. Descarga y descomprime XMLs desde PubMed Baseline
      2. Filtra los que ya están en el checkpoint
      3. Lanza workers en paralelo con ProcessPoolExecutor
      4. Muestra barra de progreso con tqdm
      5. Actualiza checkpoint conforme terminan
      6. Genera reporte final
    """
    output_dir = config["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    log = configurar_logging(output_dir)
    checkpoint = Checkpoint(output_dir)

    # ── Descargar y preparar archivos XML ───────────────────────────────────
    todos_los_xmls = preparar_xmls_desde_web(config, log)

    if not todos_los_xmls:
        log.error(f"No se pudieron preparar XMLs desde: {config['url_baseline']}")
        log.error(f"Patrón usado: {config['patron_archivos']}.gz")
        return

    # ── Filtrar pendientes (checkpoint) ─────────────────────────────────────
    pendientes = checkpoint.pendientes([str(p) for p in todos_los_xmls])

    log.info(f"{'='*60}")
    log.info(f"PROCESAMIENTO BATCH — PubMed Baseline")
    log.info(f"{'='*60}")
    log.info(f"  Total de XMLs encontrados : {len(todos_los_xmls):,}")
    log.info(f"  Ya procesados (checkpoint): {len(todos_los_xmls) - len(pendientes):,}")
    log.info(f"  Pendientes de procesar    : {len(pendientes):,}")

    if not pendientes:
        log.info("  ✓ Todos los archivos ya fueron procesados.")
        return

    # ── Configurar workers ───────────────────────────────────────────────────
    num_workers = config["num_workers"]
    if num_workers is None:
        # Usa todos los núcleos menos 1 para no congelar tu PC
        num_workers = max(1, multiprocessing.cpu_count() - 2)

    log.info(f"  Workers paralelos         : {num_workers} (de {multiprocessing.cpu_count()} núcleos)")
    log.info(f"  Corpus de salida          : {output_dir}/pubmed_corpus.jsonl")
    log.info(f"{'='*60}\n")

    # ── Preparar argumentos para cada worker ────────────────────────────────
    args_lista = [(xml_path, output_dir) for xml_path in pendientes]

    resultados = []
    total_registros = 0
    inicio_total = time.time()

    # ── Procesamiento paralelo con barra de progreso ─────────────────────────
    with ProcessPoolExecutor(max_workers=num_workers) as executor:

        futures = {
            executor.submit(procesar_un_xml, args): args[0]
            for args in args_lista
        }

        with tqdm(
            total=len(pendientes),
            desc="Procesando XMLs",
            unit="archivo",
            ncols=80,
            colour="green",
        ) as barra:

            nombres_lote = []

            for future in as_completed(futures):
                resultado = future.result()
                resultados.append(resultado)
                nombres_lote.append(resultado["nombre"])
                total_registros += resultado["registros"]

                # Actualizar checkpoint cada 10 archivos completados
                if len(nombres_lote) >= 10:
                    checkpoint.marcar_lote(nombres_lote)
                    nombres_lote = []

                # Actualizar barra de progreso
                estado = "✓" if resultado["exitoso"] else "✗"
                barra.set_postfix({
                    "archivo": resultado["nombre"][-20:],
                    "registros": f"{total_registros:,}",
                    estado: "",
                })
                barra.update(1)

                # Log de errores
                if not resultado["exitoso"]:
                    log.warning(
                        f"FALLO: {resultado['nombre']} — {resultado['error']}"
                    )

            # Guardar los últimos en checkpoint
            if nombres_lote:
                checkpoint.marcar_lote(nombres_lote)

    # ── Reporte final ────────────────────────────────────────────────────────
    tiempo_total = time.time() - inicio_total
    reporte = guardar_reporte(resultados, output_dir, tiempo_total)

    exitosos = reporte["resumen"]["exitosos"]
    fallidos = reporte["resumen"]["fallidos"]

    log.info(f"\n{'='*60}")
    log.info(f"PROCESAMIENTO COMPLETADO")
    log.info(f"{'='*60}")
    log.info(f"  Archivos procesados  : {len(resultados):,}")
    log.info(f"  Exitosos             : {exitosos:,}")
    log.info(f"  Fallidos             : {fallidos:,}")
    log.info(f"  Registros extraídos  : {total_registros:,}")
    log.info(f"  Tiempo total         : {str(timedelta(seconds=int(tiempo_total)))}")
    log.info(f"  Promedio por archivo : {reporte['resumen']['promedio_seg_por_archivo']}s")
    log.info(f"\n  Archivos de salida:")
    log.info(f"    Corpus  → {output_dir}/pubmed_corpus.jsonl")
    log.info(f"    Reporte → {output_dir}/reporte_batch.json")
    log.info(f"    Log     → {output_dir}/procesamiento.log")

    if fallidos > 0:
        log.warning(f"\n  ⚠ {fallidos} archivos fallaron.")
        log.warning(f"    Revisa reporte_batch.json para ver cuáles.")
        log.warning(f"    Puedes volver a ejecutar el script — el checkpoint")
        log.warning(f"    saltará los exitosos y reintentará los fallidos.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("""
╔══════════════════════════════════════════════════════════╗
║   PIPELINE BATCH — PubMed Baseline                       ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝

  Antes de ejecutar, edita la sección CONFIG al inicio del
  script con las rutas correctas en tu PC.

  ¿Continuar con la configuración actual? (Ctrl+C para cancelar)
    """)

    print(f"  fuente     : web")
    print(f"  url        : {CONFIG['url_baseline']}")
    print(f"  descargas  : {CONFIG['download_dir']}")
    print(f"  max archivos: {CONFIG['max_descargas']}")
    print(f"  output_dir : {CONFIG['output_dir']}")
    print(f"  patrón     : {CONFIG['patron_archivos']}")
    print()

    try:
        input("  Presiona ENTER para iniciar (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)

    ejecutar_batch(CONFIG)
