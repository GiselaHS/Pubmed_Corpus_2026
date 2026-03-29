"""
=============================================================================
RECÁLCULO DE NIVEL DE EVIDENCIA — Escala OCEBM Corregida
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Correcciones aplicadas respecto a la versión anterior:

  1. INVERSIÓN DE ESCALA (sugerencia de directora de tesis):
     La jerarquía OCEBM estándar usa Nivel 1 para evidencia más fuerte.
     Esquema anterior (incorrecto): 5=Meta-análisis, 1=Case Reports
     Esquema corregido (OCEBM):     1=Meta-análisis, 5=Case Reports

  2. JOURNAL ARTICLE FUERA DE LA JERARQUÍA (sugerencia de directora):
     "Journal Article" es un descriptor de FORMATO en MEDLINE, no de
     diseño de estudio. Se asigna Nivel 0 (sin información de diseño).
     Nivel 0 ≠ evidencia débil — significa "diseño no especificado".

Escala OCEBM adaptada:
  Nivel 1 — Más fuerte : Meta-análisis, Revisión Sistemática
  Nivel 2              : RCT, Clinical Trial Phase III
  Nivel 3              : Ensayo Clínico Controlado, Phase I/II/IV,
                         Multicéntrico, Pragmático
  Nivel 4              : Observacional, Comparativo, Review Narrativa,
                         Evaluación, Validación, Gemelos
  Nivel 5 — Más débil  : Reporte de Caso
  Nivel 0              : Tipo no especificado (Journal Article, Letter,
                         Editorial, News, financiamiento, etc.)

Uso en EC2:
    source ~/pubmed_env/bin/activate
    screen -S recalculo
    cd ~/Pubmed_Corpus_2026
    python3 recalcular_nivel_evidencia.py
=============================================================================
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, timedelta
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "sqlite_path": "/home/ubuntu/pubmed/pubmed_metadata.db",
    "batch_size":  10000,
}

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO OCEBM CORREGIDO
# Nivel 1 = más fuerte, Nivel 5 = más débil, Nivel 0 = sin info de diseño
# ─────────────────────────────────────────────────────────────────────────────

NIVELES_EVIDENCIA = {
    # Nivel 1 — Mayor síntesis
    "Systematic Review":               1,
    "Meta-Analysis":                   1,
    # Nivel 2 — Experimental aleatorizado
    "Randomized Controlled Trial":     2,
    "Clinical Trial, Phase III":       2,
    # Nivel 3 — Controlado no aleatorizado
    "Controlled Clinical Trial":       3,
    "Clinical Trial":                  3,
    "Clinical Trial, Phase I":         3,
    "Clinical Trial, Phase II":        3,
    "Clinical Trial, Phase IV":        3,
    "Multicenter Study":               3,
    "Pragmatic Clinical Trial":        3,
    # Nivel 4 — Observacional
    "Observational Study":             4,
    "Comparative Study":               4,
    "Review":                          4,
    "Evaluation Study":                4,
    "Validation Study":                4,
    "Twin Study":                      4,
    # Nivel 5 — Más débil
    "Case Reports":                    5,
    # Nivel 0 (omisión): Journal Article, Letter, Editorial, News,
    # Research Support, English Abstract, Historical Article, etc.
}


def calcular_nivel_evidencia(pub_types_json: str) -> int:
    """
    Calcula el nivel OCEBM más alto (numéricamente más bajo) del artículo.
    Retorna 0 si no hay tipos de diseño reconocidos (ej. solo Journal Article).
    """
    try:
        pub_types = json.loads(pub_types_json) if pub_types_json else []
    except (json.JSONDecodeError, TypeError):
        return 0

    nivel_min = 99
    encontrado = False
    for pt in pub_types:
        nivel = NIVELES_EVIDENCIA.get(pt)
        if nivel is not None:
            nivel_min = min(nivel_min, nivel)
            encontrado = True

    return nivel_min if encontrado else 0


# ─────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN DEL MAPEO
# ─────────────────────────────────────────────────────────────────────────────

def verificar_mapeo() -> bool:
    casos = [
        ('["Meta-Analysis", "Systematic Review"]',                     1, "Meta-Analysis + SR → nivel 1"),
        ('["Randomized Controlled Trial", "Journal Article"]',         2, "RCT + Journal Article → nivel 2"),
        ('["Clinical Trial, Phase III", "Journal Article"]',           2, "Phase III → nivel 2"),
        ('["Clinical Trial", "Journal Article"]',                      3, "Clinical Trial → nivel 3"),
        ('["Controlled Clinical Trial"]',                              3, "CCT → nivel 3"),
        ('["Observational Study", "Journal Article"]',                 4, "Observacional → nivel 4"),
        ('["Review", "Journal Article"]',                              4, "Review → nivel 4"),
        ('["Case Reports", "Journal Article"]',                        5, "Case Report → nivel 5"),
        ('["Journal Article"]',                                        0, "Solo Journal Article → nivel 0"),
        ('["Journal Article", "Research Support, Non-U.S. Gov\'t"]',  0, "Journal Article + funding → nivel 0"),
        ('["Letter"]',                                                 0, "Letter → nivel 0"),
        ('["Editorial"]',                                              0, "Editorial → nivel 0"),
        ('["News"]',                                                   0, "News → nivel 0"),
        ('["Journal Article", "Systematic Review"]',                   1, "Journal Article + SR → nivel 1 (SR gana)"),
    ]

    print("\n── Verificación del mapeo OCEBM ─────────────────────────────")
    errores = 0
    for pub_types_json, esperado, desc in casos:
        obtenido = calcular_nivel_evidencia(pub_types_json)
        ok = "✓" if obtenido == esperado else "✗"
        if obtenido != esperado:
            errores += 1
        print(f"  {ok} {desc}")
        if obtenido != esperado:
            print(f"    Esperado: {esperado} | Obtenido: {obtenido}")
    print("─────────────────────────────────────────────────────────────")
    if errores > 0:
        print(f"  ✗ {errores} casos fallaron — revisa el mapeo\n")
        return False
    print(f"  ✓ Todos los casos correctos\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def configurar_logging(sqlite_path: str):
    log_path = os.path.join(os.path.dirname(sqlite_path), "recalculo_nivel.log")
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
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_recalculo(config: dict):
    sqlite_path = config["sqlite_path"]
    batch_size  = config["batch_size"]
    log = configurar_logging(sqlite_path)

    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=20000")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM articulos")
    total = cursor.fetchone()[0]

    log.info(f"\n{'='*60}")
    log.info(f"RECÁLCULO NIVEL EVIDENCIA — Escala OCEBM Corregida")
    log.info(f"{'='*60}")
    log.info(f"  Base de datos   : {sqlite_path}")
    log.info(f"  Total registros : {total:,}")
    log.info(f"{'='*60}\n")

    # Snapshot ANTES
    labels = {
        0: "Sin info diseño",
        1: "Meta-análisis / SR",
        2: "RCT / Phase III",
        3: "Ensayo Clínico Controlado",
        4: "Observacional / Review",
        5: "Case Reports",
    }
    log.info("Distribución ANTES:")
    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) FROM articulos
        GROUP BY nivel_evidencia ORDER BY nivel_evidencia ASC
    """)
    niveles_antes = {}
    for nivel, cnt in cursor.fetchall():
        niveles_antes[nivel] = cnt
        log.info(f"  Nivel {nivel} ({labels.get(nivel,'?'):30s}): {cnt:,}")

    # Leer todos
    log.info(f"\nLeyendo {total:,} registros...")
    cursor.execute("SELECT pmid, publication_types, nivel_evidencia FROM articulos")
    todos = cursor.fetchall()

    # Calcular cambios
    log.info("Calculando nuevos niveles...")
    actualizaciones = []
    sin_cambio = 0
    for pmid, pub_types_json, nivel_actual in todos:
        nivel_nuevo = calcular_nivel_evidencia(pub_types_json)
        if nivel_nuevo != nivel_actual:
            actualizaciones.append((nivel_nuevo, pmid))
        else:
            sin_cambio += 1

    log.info(f"  A actualizar : {len(actualizaciones):,}")
    log.info(f"  Sin cambio   : {sin_cambio:,}")
    del todos

    if not actualizaciones:
        log.info("✓ No hay registros que actualizar.")
        conn.close()
        return

    # Actualizar en lotes
    log.info(f"\nActualizando en lotes de {batch_size:,}...")
    inicio = time.time()
    actualizados = 0

    with tqdm(total=len(actualizaciones), desc="Actualizando",
              unit="reg", ncols=80, colour="green") as barra:
        for i in range(0, len(actualizaciones), batch_size):
            lote = actualizaciones[i:i + batch_size]
            conn.executemany(
                "UPDATE articulos SET nivel_evidencia = ? WHERE pmid = ?",
                lote
            )
            conn.commit()
            actualizados += len(lote)
            barra.set_postfix({"actualizados": f"{actualizados:,}"})
            barra.update(len(lote))

    tiempo_total = time.time() - inicio

    # Snapshot DESPUÉS
    log.info("\nDistribución DESPUÉS (escala OCEBM corregida):")
    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) FROM articulos
        GROUP BY nivel_evidencia ORDER BY nivel_evidencia ASC
    """)
    for nivel, cnt in cursor.fetchall():
        antes = niveles_antes.get(nivel, 0)
        diff  = cnt - antes
        signo = "+" if diff >= 0 else ""
        log.info(f"  Nivel {nivel} ({labels.get(nivel,'?'):30s}): {cnt:,}  ({signo}{diff:,})")

    conn.close()

    log.info(f"\n{'='*60}")
    log.info(f"COMPLETADO")
    log.info(f"  Actualizados : {actualizados:,}")
    log.info(f"  Tiempo total : {str(timedelta(seconds=int(tiempo_total)))}")
    log.info(f"{'='*60}")
    log.info(f"\nNOTA: Nivel 0 = diseño no especificado en MEDLINE.")
    log.info(f"No significa evidencia débil. El módulo de ranking")
    log.info(f"lo trata como valor nulo en el factor de evidencia.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║   RECÁLCULO NIVEL EVIDENCIA — Escala OCEBM Corregida     ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝
  Cambios:
    1. Escala invertida → Nivel 1 = más fuerte (estándar OCEBM)
    2. Journal Article  → Nivel 0 (formato, no diseño de estudio)
    """)
    print(f"  sqlite_path : {CONFIG['sqlite_path']}")
    print()

    if not verificar_mapeo():
        exit(1)

    try:
        input("  Presiona ENTER para iniciar (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)

    ejecutar_recalculo(CONFIG)
