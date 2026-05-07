"""
=============================================================================
RECÁLCULO DE NIVEL DE EVIDENCIA — Escala OCEBM Final
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago — UJAT DACYTI

Escala OCEBM adoptada (Nivel 1 = más fuerte, Nivel 5 = más débil):

  Nivel 1 — Meta-análisis, Revisión Sistemática
  Nivel 2 — RCT, Clinical Trial Phase III
  Nivel 3 — Ensayo Clínico Controlado, Clinical Trial, Phase I/II/IV,
             Pragmatic Clinical Trial
  Nivel 4 — Observacional, Comparativo, Evaluación, Validación, Gemelos
  Nivel 5 — Reporte de Caso
  Nivel 0 — Tipo no especificado (Journal Article, Letter, Editorial,
             News, Research Support, English Abstract, etc.)

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
# ★ CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "sqlite_path": "/home/ubuntu/pubmed/pubmed_metadata.db",
    "batch_size":  10000,
}

# ─────────────────────────────────────────────────────────────────────────────
# MAPEO OCEBM FINAL
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
    "Pragmatic Clinical Trial":        3,
    # Nivel 4 — Observacional
    "Observational Study":             4,
    "Comparative Study":               4,
    "Evaluation Study":                4,
    "Validation Study":                4,
    "Twin Study":                      4,
    # Nivel 5 — Más débil
    "Case Reports":                    5,
    # Nivel 0 (omisión): Journal Article, Letter, Editorial, News,
    # Research Support, English Abstract, Historical Article, etc.
}

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN DE CÁLCULO
# ─────────────────────────────────────────────────────────────────────────────

def calcular_nivel_evidencia(pub_types_json: str) -> int:
    """
    Calcula el nivel OCEBM más fuerte (numéricamente más bajo) del artículo.

    Lógica:
      - Parsea el JSON de tipos de publicación
      - Busca cada tipo en NIVELES_EVIDENCIA
      - Retorna el mínimo encontrado (= nivel más fuerte)
      - Retorna 0 si ningún tipo está en el mapeo (diseño no especificado)

    Ejemplos:
      ["RCT", "Journal Article"] → 2  (RCT gana sobre Journal Article)
      ["Journal Article"]        → 0  (no es nivel de evidencia)
      ["Case Reports", "Letter"] → 5  (Case Reports gana sobre Letter)
    """
    try:
        pub_types = json.loads(pub_types_json) if pub_types_json else []
    except (json.JSONDecodeError, TypeError):
        return 0

    nivel_min  = 99   # centinela — ningún tipo real llega a 99
    encontrado = False

    for pt in pub_types:
        nivel = NIVELES_EVIDENCIA.get(pt)
        if nivel is not None:
            nivel_min  = min(nivel_min, nivel)
            encontrado = True

    return nivel_min if encontrado else 0


# ─────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN DEL MAPEO (corre antes de tocar la DB)
# ─────────────────────────────────────────────────────────────────────────────

def verificar_mapeo() -> bool:
    """
    Prueba unitaria con casos representativos.
    Si algún caso falla el script se detiene sin modificar la base de datos.
    """
    casos = [
        # Nivel 1
        ('["Meta-Analysis"]',                                          1, "Meta-Analysis → 1"),
        ('["Systematic Review"]',                                      1, "Systematic Review → 1"),
        ('["Meta-Analysis", "Systematic Review"]',                     1, "Meta-Analysis + SR → 1"),
        ('["Journal Article", "Systematic Review"]',                   1, "Journal Article + SR → 1 (SR gana)"),
        # Nivel 2
        ('["Randomized Controlled Trial", "Journal Article"]',         2, "RCT + Journal Article → 2"),
        ('["Clinical Trial, Phase III", "Journal Article"]',           2, "Phase III → 2"),
        # Nivel 3
        ('["Clinical Trial", "Journal Article"]',                      3, "Clinical Trial → 3"),
        ('["Controlled Clinical Trial"]',                              3, "CCT → 3"),
        ('["Clinical Trial, Phase I", "Journal Article"]',             3, "Phase I → 3"),
        ('["Pragmatic Clinical Trial", "Journal Article"]',            3, "Pragmatic → 3"),
        # Nivel 4
        ('["Observational Study", "Journal Article"]',                 4, "Observacional → 4"),
        ('["Comparative Study", "Journal Article"]',                   4, "Comparativo → 4"),
        ('["Evaluation Study", "Journal Article"]',                    4, "Evaluation → 4"),
        ('["Validation Study", "Journal Article"]',                    4, "Validation → 4"),
        ('["Twin Study", "Journal Article"]',                          4, "Twin Study → 4"),
        # Nivel 5
        ('["Case Reports", "Journal Article"]',                        5, "Case Reports → 5"),
        # Nivel 0
        ('["Journal Article"]',                                        0, "Solo Journal Article → 0"),
        ('["Journal Article", "Research Support, Non-U.S. Gov\'t"]',  0, "Journal Article + funding → 0"),
        ('["Letter"]',                                                 0, "Letter → 0"),
        ('["Editorial"]',                                              0, "Editorial → 0"),
        ('["News"]',                                                   0, "News → 0"),
        ('["Comment", "Letter"]',                                      0, "Comment + Letter → 0"),
        # Múltiples tipos — el más fuerte gana
        ('["Randomized Controlled Trial", "Multicenter Study", "Journal Article"]',
                                                                       2, "RCT + Multicenter → 2 (RCT gana)"),
        ('["Case Reports", "Observational Study"]',                    4, "Case Reports + Observacional → 4 (Obs. gana)"),
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
            print(f"      Esperado: {esperado} | Obtenido: {obtenido}  ← ERROR")
    print("─────────────────────────────────────────────────────────────")

    if errores > 0:
        print(f"  ✗ {errores} caso(s) fallaron — corrige el mapeo antes de continuar\n")
        return False

    print(f"  ✓ {len(casos)} casos correctos — mapeo validado\n")
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

    labels = {
        0: "Sin info diseño (Journal Article, etc.)",
        1: "Meta-análisis / Revisión Sistemática",
        2: "RCT / Clinical Trial Phase III",
        3: "Ensayo Clínico Controlado / Clinical Trial",
        4: "Observacional / Comparativo / Evaluación",
        5: "Reporte de Caso",
    }

    log.info(f"\n{'='*60}")
    log.info(f"RECÁLCULO NIVEL EVIDENCIA — Escala OCEBM Final")
    log.info(f"{'='*60}")
    log.info(f"  Base de datos   : {sqlite_path}")
    log.info(f"  Total registros : {total:,}")
    log.info(f"{'='*60}\n")

    # ── Snapshot ANTES ───────────────────────────────────────────────────────
    log.info("Distribución ANTES del recálculo:")
    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) FROM articulos
        GROUP BY nivel_evidencia ORDER BY nivel_evidencia ASC
    """)
    niveles_antes = {}
    for nivel, cnt in cursor.fetchall():
        niveles_antes[nivel] = cnt
        log.info(f"  Nivel {nivel} — {labels.get(nivel,'?'):45s}: {cnt:,}")

    # ── Leer todos los registros ─────────────────────────────────────────────
    log.info(f"\nLeyendo {total:,} registros en memoria...")
    cursor.execute("SELECT pmid, publication_types, nivel_evidencia FROM articulos")
    todos = cursor.fetchall()

    # ── Calcular nuevos niveles ───────────────────────────────────────────────
    log.info("Calculando nuevos niveles OCEBM...")
    actualizaciones = []
    sin_cambio      = 0

    for pmid, pub_types_json, nivel_actual in todos:
        nivel_nuevo = calcular_nivel_evidencia(pub_types_json)
        if nivel_nuevo != nivel_actual:
            actualizaciones.append((nivel_nuevo, pmid))
        else:
            sin_cambio += 1

    log.info(f"  Registros a actualizar : {len(actualizaciones):,}")
    log.info(f"  Registros sin cambio   : {sin_cambio:,}")
    del todos  # liberar RAM

    if not actualizaciones:
        log.info("\n✓ Todos los registros ya tienen el nivel correcto.")
        conn.close()
        return

    # ── Actualizar en lotes ───────────────────────────────────────────────────
    log.info(f"\nActualizando {len(actualizaciones):,} registros en lotes de {batch_size:,}...")
    inicio       = time.time()
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

    # ── Snapshot DESPUÉS ─────────────────────────────────────────────────────
    log.info("\nDistribución DESPUÉS del recálculo:")
    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) FROM articulos
        GROUP BY nivel_evidencia ORDER BY nivel_evidencia ASC
    """)
    for nivel, cnt in cursor.fetchall():
        antes = niveles_antes.get(nivel, 0)
        diff  = cnt - antes
        signo = "+" if diff >= 0 else ""
        log.info(
            f"  Nivel {nivel} — {labels.get(nivel,'?'):45s}: "
            f"{cnt:,}  ({signo}{diff:,})"
        )

    conn.close()

    log.info(f"\n{'='*60}")
    log.info(f"RECÁLCULO COMPLETADO")
    log.info(f"  Registros actualizados : {actualizados:,}")
    log.info(f"  Tiempo total           : {str(timedelta(seconds=int(tiempo_total)))}")
    log.info(f"{'='*60}")
    log.info(f"\nNOTA IMPORTANTE:")
    log.info(f"  Nivel 0 = diseño de estudio no especificado en MEDLINE")
    log.info(f"  (Journal Article es descriptor de formato, no de diseño)")
    log.info(f"  El módulo de ranking lo trata como valor nulo.")


# ─────────────────────────────────────────────────────────────────────────────
# CONSULTAS DE VERIFICACIÓN (para ejecutar después del recálculo)
# ─────────────────────────────────────────────────────────────────────────────

def consultar_resultados(sqlite_path: str):
    """
    Muestra las estadísticas finales del recálculo.
    Ejecutar después de que termine el proceso principal.
    """
    conn   = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    labels = {
        0: "Sin info diseño",
        1: "Meta-análisis / SR",
        2: "RCT / Phase III",
        3: "Ensayo Clínico Controlado",
        4: "Observacional / Comparativo",
        5: "Reporte de Caso",
    }

    print("\n══════════════════════════════════════════════════════════")
    print("RESULTADOS FINALES — Distribución por Nivel de Evidencia")
    print("══════════════════════════════════════════════════════════")

    cursor.execute("SELECT COUNT(*) FROM articulos")
    total = cursor.fetchone()[0]
    print(f"\n  Total registros: {total:,}\n")

    cursor.execute("""
        SELECT nivel_evidencia, COUNT(*) as cnt
        FROM articulos
        GROUP BY nivel_evidencia
        ORDER BY nivel_evidencia ASC
    """)
    for nivel, cnt in cursor.fetchall():
        pct   = cnt / total * 100
        barra = "█" * int(pct / 2)
        print(f"  Nivel {nivel} [{barra:<25}] {cnt:>12,}  ({pct:5.2f}%)")
        print(f"         {labels.get(nivel,'')}")

    print("\n── Artículos con mayor RCR por nivel ────────────────────")
    for nivel in [1, 2, 3]:
        cursor.execute("""
            SELECT pmid, titulo, rcr, citation_count
            FROM articulos
            WHERE nivel_evidencia = ? AND rcr IS NOT NULL
            ORDER BY rcr DESC LIMIT 3
        """, (nivel,))
        rows = cursor.fetchall()
        if rows:
            print(f"\n  Top 3 Nivel {nivel} por RCR:")
            for pmid, titulo, rcr, citas in rows:
                print(f"    PMID {pmid} | RCR={rcr:.2f} | citas={citas:,}")
                print(f"    {titulo[:70]}...")

    conn.close()
    print("\n══════════════════════════════════════════════════════════\n")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════╗
║   RECÁLCULO NIVEL EVIDENCIA — Escala OCEBM Final         ║
║   Recuperación Inteligente de Evidencia Científica       ║
║   Gisela Hernández Santiago — UJAT DACYTI                ║
╚══════════════════════════════════════════════════════════╝
  Escala:
    Nivel 1 — Meta-análisis / Revisión Sistemática
    Nivel 2 — RCT / Clinical Trial Phase III
    Nivel 3 — Ensayo Clínico Controlado / Clinical Trial
    Nivel 4 — Observacional / Comparativo / Evaluación
    Nivel 5 — Reporte de Caso
    Nivel 0 — Tipo no especificado (Journal Article, etc.)
    """)
    print(f"  sqlite_path : {CONFIG['sqlite_path']}")
    print()

    # Paso 1: verificar el mapeo antes de tocar la DB
    if not verificar_mapeo():
        exit(1)

    # Paso 2: confirmar antes de ejecutar
    try:
        input("  Presiona ENTER para iniciar el recálculo (Ctrl+C para cancelar)...\n")
    except KeyboardInterrupt:
        print("\n  Cancelado.")
        exit(0)

    # Paso 3: ejecutar recálculo
    ejecutar_recalculo(CONFIG)

    # Paso 4: mostrar resultados finales
    print("\n¿Mostrar resultados finales con distribución completa? (s/n): ", end="")
    if input().strip().lower() == "s":
        consultar_resultados(CONFIG["sqlite_path"])
