"""
=============================================================================
T2 + T3 — SQLite de Metadatos + Enriquecimiento iCite (multi-JSONL)
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Qué hace:
  1. Lee todos los .jsonl del corpus (un archivo por XML de PubMed)
  2. Pasada 1: procesa registros CON abstract + consulta iCite
  3. Pasada 2: procesa registros SIN abstract + consulta iCite
  4. Construye pubmed_metadata.db con metadatos + citas

Uso en EC2:
    source ~/pubmed_env/bin/activate
    screen -S icite
    cd ~/Pubmed_Corpus_2026
    python3 t2_t3_sqlite_icite.py
=============================================================================
"""

import os
import json
import time
import sqlite3
import logging
import requests
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

CONFIG = {
    "corpus_dir":      "/home/ubuntu/pubmed/corpus",
    "sqlite_path":     "/home/ubuntu/pubmed/pubmed_metadata.db",
    "checkpoint_path": "/home/ubuntu/pubmed/icite_checkpoint.json",
    "batch_size":      100,
    "delay_segundos":  0.15,
    "max_reintentos":  3,
}

ICITE_URL = "https://icite.od.nih.gov/api/pubs"


def configurar_logging(output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    log_path = os.path.join(output_dir, "t2_t3_proceso.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


class Checkpoint:
    def __init__(self, ruta: str):
        self.ruta = ruta
        self.data = self._cargar()

    def _cargar(self) -> dict:
        if os.path.exists(self.ruta):
            with open(self.ruta, "r") as f:
                data = json.load(f)
            print(f"  ✓ Checkpoint: {data.get('total_insertados', 0):,} registros ya insertados")
            return data
        return {"archivos_procesados": [], "total_insertados": 0}

    def guardar(self, clave: str, total: int):
        if clave not in self.data["archivos_procesados"]:
            self.data["archivos_procesados"].append(clave)
        self.data["total_insertados"] = total
        self.data["ultima_actualizacion"] = datetime.now().isoformat()
        with open(self.ruta, "w") as f:
            json.dump(self.data, f, indent=2)

    def ya_procesado(self, clave: str) -> bool:
        return clave in self.data.get("archivos_procesados", [])

    @property
    def total_insertados(self) -> int:
        return self.data.get("total_insertados", 0)


def crear_base_datos(sqlite_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articulos (
            pmid                TEXT PRIMARY KEY,
            titulo              TEXT,
            tiene_abstract      INTEGER,
            nivel_evidencia     INTEGER,
            anio                TEXT,
            journal             TEXT,
            issn                TEXT,
            doi                 TEXT,
            idioma              TEXT,
            pais_publicacion    TEXT,
            publication_types   TEXT,
            mesh_tags           TEXT,
            citation_count      INTEGER,
            rcr                 REAL,
            is_clinical_trial   INTEGER,
            expected_cit_year   REAL,
            icite_consultado    INTEGER DEFAULT 0,
            fecha_insercion     TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anio     ON articulos(anio)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nivel    ON articulos(nivel_evidencia)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rcr      ON articulos(rcr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_abstract ON articulos(tiene_abstract)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_icite    ON articulos(icite_consultado)")
    conn.commit()
    return conn


def consultar_icite(pmids: list, max_reintentos: int) -> dict:
    pmids_str = ",".join(str(p) for p in pmids)
    for intento in range(1, max_reintentos + 1):
        try:
            resp = requests.get(ICITE_URL, params={"pmids": pmids_str}, timeout=30)
            resp.raise_for_status()
            resultado = {}
            for pub in resp.json().get("data", []):
                pmid = str(pub.get("pmid", ""))
                if pmid:
                    resultado[pmid] = {
                        "citation_count":    pub.get("citation_count", 0),
                        "rcr":               pub.get("relative_citation_ratio"),
                        "is_clinical_trial": 1 if pub.get("is_clinical_trial") else 0,
                        "expected_cit_year": pub.get("expected_citations_per_year"),
                    }
            return resultado
        except Exception:
            if intento < max_reintentos:
                time.sleep(2 ** intento)
    return {}


def insertar_lote(cursor, registros: list, datos_icite: dict, fecha: str):
    filas = []
    for reg in registros:
        pmid  = reg["pmid"]
        citas = datos_icite.get(pmid, {})
        filas.append((
            pmid,
            reg.get("titulo", ""),
            1 if reg.get("tiene_abstract") else 0,
            reg.get("nivel_evidencia", 1),
            reg.get("anio", "N/A"),
            reg.get("journal", ""),
            reg.get("issn", ""),
            reg.get("doi", ""),
            reg.get("idioma", ""),
            reg.get("pais_publicacion", ""),
            json.dumps(reg.get("publication_types", []), ensure_ascii=False),
            json.dumps(reg.get("mesh_tags", []),         ensure_ascii=False),
            citas.get("citation_count"),
            citas.get("rcr"),
            citas.get("is_clinical_trial"),
            citas.get("expected_cit_year"),
            1 if citas else 0,
            fecha,
        ))
    cursor.executemany("""
        INSERT OR IGNORE INTO articulos (
            pmid, titulo, tiene_abstract, nivel_evidencia,
            anio, journal, issn, doi, idioma, pais_publicacion,
            publication_types, mesh_tags,
            citation_count, rcr, is_clinical_trial, expected_cit_year,
            icite_consultado, fecha_insercion
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, filas)


def procesar_pasada(
    jsonl_files: list, conn: sqlite3.Connection, config: dict,
    total_insertados: int, barra: tqdm, solo_con_abstract: bool,
    checkpoint: Checkpoint, fecha: str, prefijo: str,
) -> int:
    cursor     = conn.cursor()
    lote_reg   = []
    lote_pmids = []

    def vaciar_lote():
        nonlocal total_insertados
        if not lote_reg:
            return
        datos_icite = consultar_icite(lote_pmids, config["max_reintentos"])
        time.sleep(config["delay_segundos"])
        insertar_lote(cursor, lote_reg, datos_icite, fecha)
        conn.commit()
        total_insertados += len(lote_reg)
        lote_reg.clear()
        lote_pmids.clear()

    for jsonl_path in jsonl_files:
        clave = f"{prefijo}_{jsonl_path.name}"
        if checkpoint.ya_procesado(clave):
            continue

        with open(str(jsonl_path), "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    reg = json.loads(linea)
                except json.JSONDecodeError:
                    continue

                tiene = bool(reg.get("tiene_abstract"))
                if solo_con_abstract and not tiene:
                    continue
                if not solo_con_abstract and tiene:
                    continue

                lote_reg.append(reg)
                lote_pmids.append(reg["pmid"])

                if len(lote_pmids) >= config["batch_size"]:
                    vaciar_lote()
                    barra.set_postfix({
                        "insertados": f"{total_insertados:,}",
                        "archivo": jsonl_path.stem[-12:],
                    })

                barra.update(1)

        vaciar_lote()
        checkpoint.guardar(clave, total_insertados)

    return total_insertados


def ejecutar_t2_t3(config: dict):
    corpus_dir  = config["corpus_dir"]
    sqlite_path = config["sqlite_path"]

    log = configurar_logging(os.path.dirname(sqlite_path))

    jsonl_files = sorted(Path(corpus_dir).glob("pubmed26n*.jsonl"))
    if not jsonl_files:
        log.error(f"No se encontraron archivos .jsonl en: {corpus_dir}")
        return

    checkpoint = Checkpoint(config["checkpoint_path"])
    conn       = crear_base_datos(sqlite_path)
    fecha_hoy  = datetime.now().strftime("%Y-%m-%d")

    # Contar registros
    log.info(f"Contando registros en {len(jsonl_files):,} archivos .jsonl...")
    con_abstract = sin_abstract = 0
    for f in jsonl_files:
        with open(str(f), "r", encoding="utf-8") as fh:
            for linea in fh:
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    reg = json.loads(linea)
                    if reg.get("tiene_abstract"):
                        con_abstract += 1
                    else:
                        sin_abstract += 1
                except json.JSONDecodeError:
                    continue

    total = con_abstract + sin_abstract
    log.info(f"\n{'='*60}")
    log.info(f"T2 + T3 — SQLite + iCite  (multi-JSONL)")
    log.info(f"{'='*60}")
    log.info(f"  Archivos .jsonl   : {len(jsonl_files):,}")
    log.info(f"  Con abstract      : {con_abstract:,}")
    log.info(f"  Sin abstract      : {sin_abstract:,}")
    log.info(f"  Total             : {total:,}")
    log.info(f"  Requests iCite    : ~{total // 100:,}")
    log.info(f"  Tiempo estimado   : ~{total * 0.0015 / 3600:.1f} horas")
    log.info(f"  SQLite destino    : {sqlite_path}")
    log.info(f"{'='*60}\n")

    total_insertados = checkpoint.total_insertados

    # ── Pasada 1: CON abstract ───────────────────────────────────────────────
    log.info("PASADA 1 — Registros CON abstract + iCite")
    with tqdm(total=con_abstract, desc="Con abstract",
              unit="reg", ncols=80, colour="green") as barra:
        total_insertados = procesar_pasada(
            jsonl_files, conn, config, total_insertados,
            barra, solo_con_abstract=True,
            checkpoint=checkpoint, fecha=fecha_hoy, prefijo="abstract",
        )
    log.info(f"  Pasada 1 completada — {total_insertados:,} registros")

    # ── Pasada 2: SIN abstract ───────────────────────────────────────────────
    log.info("\nPASADA 2 — Registros SIN abstract + iCite")
    with tqdm(total=sin_abstract, desc="Sin abstract",
              unit="reg", ncols=80, colour="yellow") as barra:
        total_insertados = procesar_pasada(
            jsonl_files, conn, config, total_insertados,
            barra, solo_con_abstract=False,
            checkpoint=checkpoint, fecha=fecha_hoy, prefijo="noabstract",
        )
    log.info(f"  Pasada 2 completada — {total_insertados:,} registros")

    # ── Estadísticas finales ─────────────────────────────────────────────────
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articulos")
    total_db = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articulos WHERE icite_consultado=1")
    con_icite = cursor.fetchone()[0]
    cursor.execute("SELECT AVG(rcr) FROM articulos WHERE rcr IS NOT NULL")
    avg_rcr = cursor.fetchone()[0]
    conn.close()

    log.info(f"\n{'='*60}")
    log.info(f"COMPLETADO")
    log.info(f"  Total en SQLite   : {total_db:,}")
    log.info(f"  Con iCite         : {con_icite:,}")
    log.info(f"  RCR promedio      : {avg_rcr:.3f}" if avg_rcr else "  RCR promedio      : N/A")
    log.info(f"  Base de datos     : {sqlite_path}")


def verificar_db(sqlite_path: str):
    conn   = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    print("\n── Totales ──────────────────────────────────────────────")
    cursor.execute("SELECT COUNT(*) FROM articulos")
    print(f"  Total             : {cursor.fetchone()[0]:,}")
    cursor.execute("SELECT COUNT(*) FROM articulos WHERE tiene_abstract=1")
    print(f"  Con abstract      : {cursor.fetchone()[0]:,}")
    cursor.execute("SELECT COUNT(*) FROM articulos WHERE icite_consultado=1")
    print(f"  Con iCite         : {cursor.fetchone()[0]:,}")
    print("\n── Nivel de evidencia ───────────────────────────────────")
    niveles = {1:"Journal Article", 2:"Review/Obs.", 3:"Clinical Trial",
               4:"RCT", 5:"Meta-análisis/SR"}
    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) FROM articulos
        GROUP BY nivel_evidencia ORDER BY nivel_evidencia DESC
    """)
    for nivel, total in cursor.fetchall():
        print(f"  Nivel {nivel} ({niveles.get(nivel,'?'):15s}): {total:,}")
    print("\n── Top 5 por RCR ────────────────────────────────────────")
    cursor.execute("""
        SELECT pmid, anio, rcr, citation_count, titulo FROM articulos
        WHERE rcr IS NOT NULL ORDER BY rcr DESC LIMIT 5
    """)
    for pmid, anio, rcr, citas, titulo in cursor.fetchall():
        print(f"  PMID {pmid} ({anio}) RCR={rcr:.2f} citas={citas}")
        print(f"    {titulo[:65]}...")
    conn.close()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║   T2 + T3 — SQLite + iCite  (multi-JSONL)               ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝
    """)
    print(f"  corpus_dir  : {CONFIG['corpus_dir']}")
    print(f"  sqlite_path : {CONFIG['sqlite_path']}")
    print()
    try:
        input("  Presiona ENTER para iniciar (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)

    ejecutar_t2_t3(CONFIG)

    print("\n¿Ejecutar verificación? (s/n): ", end="")
    if input().strip().lower() == "s":
        verificar_db(CONFIG["sqlite_path"])
