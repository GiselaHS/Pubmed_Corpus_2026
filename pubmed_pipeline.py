"""
=============================================================================
PIPELINE DE EXTRACCIÓN Y CONSTRUCCIÓN DE DATASET - PubMed Baseline
=============================================================================
Proyecto: Recuperación Inteligente de Evidencia Científica mediante PLN
Autora:   Gisela Hernández Santiago
Maestría: Ciencias de la Computación — UJAT DACYTI

Pipeline completo:
  1. Extracción robusta de XMLs de PubMed (un archivo o en batch)
  2. Limpieza y normalización de texto
  3. Exportación a JSON Lines (.jsonl) — formato óptimo para grandes volúmenes
  4. Construcción de índice BM25 con Pyserini
  5. Estadísticas y reporte del dataset

Uso en Kaggle / Colab:
    python pubmed_pipeline.py --input_dir /ruta/xmls --output_dir /ruta/salida
=============================================================================
"""

import xml.etree.ElementTree as ET
import json
import re
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES: Tipos de publicación relevantes para MBE (criterio de calidad)
# ─────────────────────────────────────────────────────────────────────────────
# Jerarquía de evidencia (mayor número = mayor calidad metodológica)
NIVELES_EVIDENCIA = {
    "Systematic Review":          5,
    "Meta-Analysis":              5,
    "Randomized Controlled Trial": 4,
    "Controlled Clinical Trial":  3,
    "Clinical Trial":             3,
    "Multicenter Study":          3,
    "Comparative Study":          2,
    "Observational Study":        2,
    "Review":                     2,
    "Case Reports":               1,
    "Journal Article":            1,
}


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES DE LIMPIEZA
# ─────────────────────────────────────────────────────────────────────────────

def limpiar_texto(texto: str) -> str:
    """
    Limpia texto de etiquetas XML/HTML residuales y espacios múltiples.
    Maneja None de forma segura.
    """
    if not texto:
        return ""
    # Elimina etiquetas XML/HTML (p.ej. <i>, <sup>, <b>, etc.)
    limpio = re.sub(r'<[^>]+>', ' ', texto)
    # Elimina caracteres de control excepto saltos de línea
    limpio = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', limpio)
    # Normaliza espacios múltiples
    limpio = re.sub(r'\s+', ' ', limpio)
    return limpio.strip()


def extraer_texto_nodo(nodo) -> str:
    """
    Extrae TODO el texto de un nodo XML, incluyendo el texto de sub-elementos.
    Soluciona el problema de ArticleTitle con sub-etiquetas como <i> o <sup>.
    
    Ejemplo:
        <ArticleTitle>Effect of <i>BRCA1</i> mutation on survival</ArticleTitle>
        → "Effect of BRCA1 mutation on survival"
    """
    if nodo is None:
        return ""
    # itertext() recorre el nodo principal Y todos sus descendientes en orden
    texto_completo = "".join(nodo.itertext())
    return limpiar_texto(texto_completo)


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE ABSTRACT ESTRUCTURADO
# ─────────────────────────────────────────────────────────────────────────────

def extraer_abstract(articulo) -> dict:
    """
    Extrae el abstract de forma inteligente.
    
    Maneja dos formatos de PubMed:
    
    1. Abstract simple:
       <AbstractText>Texto del resumen...</AbstractText>
    
    2. Abstract estructurado (el más común en ensayos clínicos):
       <AbstractText Label="BACKGROUND">...</AbstractText>
       <AbstractText Label="METHODS">...</AbstractText>
       <AbstractText Label="RESULTS">...</AbstractText>
       <AbstractText Label="CONCLUSIONS">...</AbstractText>
    
    Retorna:
        {
          "abstract_full": "texto completo concatenado",
          "abstract_sections": {"BACKGROUND": "...", "METHODS": "...", ...},
          "tiene_abstract": True/False
        }
    """
    nodos = articulo.findall('.//AbstractText')
    
    if not nodos:
        return {
            "abstract_full": "",
            "abstract_sections": {},
            "tiene_abstract": False
        }
    
    secciones = {}
    partes = []
    
    for nodo in nodos:
        texto = extraer_texto_nodo(nodo)
        if not texto:
            continue
        
        label = nodo.get('Label', '').strip().upper()
        
        if label:
            # Abstract estructurado: guardamos sección con su etiqueta
            secciones[label] = texto
            partes.append(f"{label}: {texto}")
        else:
            # Abstract simple: sin etiqueta
            partes.append(texto)
    
    abstract_full = " ".join(partes)
    
    return {
        "abstract_full": abstract_full if abstract_full else "",
        "abstract_sections": secciones,
        "tiene_abstract": bool(abstract_full)
    }


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACCIÓN DE AÑO DE PUBLICACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def extraer_anio(articulo) -> str:
    """
    Extrae el año de publicación con múltiples estrategias de fallback.
    
    Jerarquía de búsqueda:
    1. PubDate/Year          → año más preciso del artículo original
    2. PubDate/MedlineDate   → fallback para fechas tipo "2019 Jan-Feb"
    3. ArticleDate[@DateType='Electronic']/Year → fecha de publicación online
    4. DateCompleted/Year    → fecha en que MEDLINE completó el registro
    """
    # Estrategia 1: Año directo
    year_node = articulo.find('.//PubDate/Year')
    if year_node is not None and year_node.text:
        return year_node.text.strip()
    
    # Estrategia 2: MedlineDate (formato "2019 Jan" o "2019 Jan-Feb")
    medline_node = articulo.find('.//PubDate/MedlineDate')
    if medline_node is not None and medline_node.text:
        match = re.search(r'\b(19|20)\d{2}\b', medline_node.text)
        if match:
            return match.group()
    
    # Estrategia 3: Fecha electrónica
    edate_node = articulo.find('.//ArticleDate[@DateType="Electronic"]/Year')
    if edate_node is not None and edate_node.text:
        return edate_node.text.strip()
    
    # Estrategia 4: Fecha completada en MEDLINE
    completed_node = articulo.find('.//DateCompleted/Year')
    if completed_node is not None and completed_node.text:
        return completed_node.text.strip()
    
    return "N/A"


# ─────────────────────────────────────────────────────────────────────────────
# CÁLCULO DE NIVEL DE EVIDENCIA (para el ranking ponderado)
# ─────────────────────────────────────────────────────────────────────────────

def calcular_nivel_evidencia(pub_types: list) -> int:
    """
    Determina el nivel de evidencia más alto según los tipos de publicación.
    Usado en el módulo de ranking ponderado (criterio bibliométrico).
    
    Retorna un entero de 1 (baja) a 5 (alta evidencia).
    """
    nivel_max = 0
    for pt in pub_types:
        nivel = NIVELES_EVIDENCIA.get(pt, 0)
        nivel_max = max(nivel_max, nivel)
    return nivel_max if nivel_max > 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN
# ─────────────────────────────────────────────────────────────────────────────

def extraer_articulos_de_xml(file_path: str) -> list:
    """
    Parsea un archivo XML de PubMed y extrae todos los registros.
    
    Campos extraídos:
        - pmid           : Identificador único de PubMed
        - titulo         : Título completo (con sub-etiquetas resueltas)
        - abstract_full  : Texto completo del resumen
        - abstract_sections: Secciones del abstract (si es estructurado)
        - tiene_abstract : Flag boolean
        - publication_types: Lista de tipos de publicación
        - nivel_evidencia: Nivel de evidencia metodológica (1-5)
        - mesh_tags      : Términos MeSH asignados
        - keywords       : Palabras clave del autor
        - journal        : Nombre de la revista
        - issn           : ISSN de la revista
        - anio           : Año de publicación
        - pais_publicacion: País de la revista
        - idioma         : Idioma del artículo
        - doi            : DOI si está disponible
        - texto_indexable: Concatenación título + abstract (para BM25/embeddings)
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        log.error(f"Error parseando {file_path}: {e}")
        return []
    
    lista_articulos = []
    
    for articulo in root.findall('PubmedArticle'):
        try:
            # ── PMID ──────────────────────────────────────────────────────────
            pmid_node = articulo.find('.//PMID')
            pmid = pmid_node.text.strip() if pmid_node is not None else "N/A"
            
            # ── TÍTULO (con itertext para sub-etiquetas) ──────────────────────
            title_node = articulo.find('.//ArticleTitle')
            titulo = extraer_texto_nodo(title_node) if title_node is not None else ""
            if not titulo:
                titulo = "Sin título"
            
            # ── ABSTRACT (estructurado o simple) ─────────────────────────────
            abstract_data = extraer_abstract(articulo)
            
            # ── TIPOS DE PUBLICACIÓN ──────────────────────────────────────────
            pub_types = [
                pt.text.strip()
                for pt in articulo.findall('.//PublicationType')
                if pt.text
            ]
            
            # ── NIVEL DE EVIDENCIA (para ranking) ────────────────────────────
            nivel_evidencia = calcular_nivel_evidencia(pub_types)
            
            # ── TÉRMINOS MESH ─────────────────────────────────────────────────
            mesh_tags = [
                m.text.strip()
                for m in articulo.findall('.//MeshHeading/DescriptorName')
                if m.text
            ]
            
            # ── KEYWORDS DEL AUTOR ───────────────────────────────────────────
            keywords = [
                kw.text.strip()
                for kw in articulo.findall('.//Keyword')
                if kw.text
            ]
            
            # ── JOURNAL ──────────────────────────────────────────────────────
            journal_node = articulo.find('.//Journal/Title')
            journal = journal_node.text.strip() if journal_node is not None and journal_node.text else ""
            
            issn_node = articulo.find('.//Journal/ISSN')
            issn = issn_node.text.strip() if issn_node is not None and issn_node.text else ""
            
            # ── AÑO DE PUBLICACIÓN ────────────────────────────────────────────
            anio = extraer_anio(articulo)
            
            # ── PAÍS DE PUBLICACIÓN ───────────────────────────────────────────
            pais_node = articulo.find('.//MedlineJournalInfo/Country')
            pais = pais_node.text.strip() if pais_node is not None and pais_node.text else ""
            
            # ── IDIOMA ────────────────────────────────────────────────────────
            lang_nodes = articulo.findall('.//Language')
            idioma = lang_nodes[0].text.strip() if lang_nodes else "eng"
            
            # ── DOI ───────────────────────────────────────────────────────────
            doi = ""
            for eid in articulo.findall('.//ArticleId'):
                if eid.get('IdType') == 'doi' and eid.text:
                    doi = eid.text.strip()
                    break
            
            # ── TEXTO INDEXABLE (título + abstract para BM25 y BioBERT) ──────
            # Este campo es el que se indexa — combina título y abstract
            texto_indexable = f"{titulo} {abstract_data['abstract_full']}".strip()
            
            lista_articulos.append({
                "pmid":              pmid,
                "titulo":            titulo,
                "abstract_full":     abstract_data["abstract_full"],
                "abstract_sections": abstract_data["abstract_sections"],
                "tiene_abstract":    abstract_data["tiene_abstract"],
                "publication_types": pub_types,
                "nivel_evidencia":   nivel_evidencia,
                "mesh_tags":         mesh_tags,
                "keywords":          keywords,
                "journal":           journal,
                "issn":              issn,
                "anio":              anio,
                "pais_publicacion":  pais,
                "idioma":            idioma,
                "doi":               doi,
                "texto_indexable":   texto_indexable,
            })
        
        except Exception as e:
            log.warning(f"Error procesando artículo en {file_path}: {e}")
            continue
    
    return lista_articulos


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTACIÓN A JSON LINES (.jsonl)
# ─────────────────────────────────────────────────────────────────────────────

def guardar_jsonl(articulos: list, ruta_salida: str, modo: str = 'a') -> int:
    """
    Guarda artículos en formato JSON Lines (un JSON por línea).
    
    ¿Por qué JSONL en lugar de JSON?
    - Permite procesar archivo por archivo y ACUMULAR en un solo archivo
    - No requiere cargar TODO en memoria para leer/escribir
    - Compatible directamente con Pyserini para indexación BM25
    - Permite streaming (lectura línea por línea) para datasets de 20M+ registros
    
    Args:
        articulos   : Lista de diccionarios con los artículos
        ruta_salida : Ruta del archivo .jsonl de destino
        modo        : 'a' para acumular (append), 'w' para sobreescribir
    
    Retorna:
        Número de registros escritos
    """
    escritos = 0
    with open(ruta_salida, modo, encoding='utf-8') as f:
        for art in articulos:
            f.write(json.dumps(art, ensure_ascii=False) + '\n')
            escritos += 1
    return escritos


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def ejecutar_pipeline(input_dir: str, output_dir: str, patron: str = "*.xml"):
    """
    Pipeline completo:
      1. Encuentra todos los XMLs en input_dir
      2. Extrae artículos de cada XML
      3. Acumula en un único archivo .jsonl
      4. Genera estadísticas
      5. Prepara colección para Pyserini
    
    Args:
        input_dir : Directorio con archivos XML de PubMed
        output_dir: Directorio para los archivos de salida
        patron    : Patrón glob para filtrar archivos (default: "*.xml")
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Rutas de salida
    ruta_jsonl      = os.path.join(output_dir, "pubmed_corpus.jsonl")
    
    # Encontrar archivos XML
    xmls = sorted(Path(input_dir).glob(patron))
    
    if not xmls:
        log.error(f"No se encontraron archivos con patrón '{patron}' en {input_dir}")
        return
    
    log.info(f"Se encontraron {len(xmls)} archivos XML para procesar")
    
    total_registros = 0
    
    # ── PASO 1: Extracción y acumulación en JSONL ─────────────────────────────
    for i, xml_path in enumerate(xmls, 1):
        log.info(f"[{i}/{len(xmls)}] Procesando: {xml_path.name}")
        
        articulos = extraer_articulos_de_xml(str(xml_path))
        
        if articulos:
            escritos = guardar_jsonl(articulos, ruta_jsonl, modo='a')
            total_registros += escritos
            log.info(f"  ✓ {escritos:,} registros extraídos — Total acumulado: {total_registros:,}")
        else:
            log.warning(f"  ✗ Sin registros en {xml_path.name}")
    
    log.info(f"\n{'='*60}")
    log.info(f"EXTRACCIÓN COMPLETADA — {total_registros:,} registros totales")
    log.info(f"Corpus guardado en: {ruta_jsonl}")
    

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT (línea de comandos)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline de extracción y construcción de dataset PubMed"
    )
    parser.add_argument(
        "--input_dir",  required=True,
        help="Directorio con archivos XML de PubMed"
    )
    parser.add_argument(
        "--output_dir", required=True,
        help="Directorio de salida para el corpus y el índice"
    )
    parser.add_argument(
        "--patron", default="*.xml",
        help="Patrón de archivos a procesar (default: *.xml)"
    )
    parser.add_argument(
        "--archivo_unico",
        help="Procesar solo un archivo XML (modo prueba)"
    )
    
    args = parser.parse_args()
    
    if args.archivo_unico:
        procesar_archivo_unico(args.archivo_unico, args.output_dir)
    else:
        ejecutar_pipeline(args.input_dir, args.output_dir, args.patron)

"""
=============================================================================
ADICIÓN A pubmed_pipeline.py — Parseo desde bytes en memoria
=============================================================================
Agrega esta función a tu pubmed_pipeline.py existente.

El script descarga_ftp.py la importa para procesar los XMLs
descomprimidos en memoria sin guardarlos en disco.
=============================================================================
"""

import xml.etree.ElementTree as ET
from io import BytesIO

# Copia aquí las funciones existentes de pubmed_pipeline.py
# (limpiar_texto, extraer_texto_nodo, extraer_abstract,
#  extraer_anio, calcular_nivel_evidencia, guardar_jsonl)
# y agrega esta función nueva:


def extraer_articulos_de_xml_desde_bytes(xml_bytes: bytes) -> list:
    """
    Versión de extraer_articulos_de_xml() que trabaja con bytes en memoria
    en lugar de un archivo en disco.

    Usada por descarga_ftp.py para procesar XMLs descomprimidos en RAM,
    sin necesidad de guardar el archivo .gz ni el .xml en disco.

    Args:
        xml_bytes : Contenido del XML como bytes (ya descomprimido del .gz)

    Retorna:
        Lista de diccionarios con los campos de cada artículo
    """
    try:
        # Parsear desde bytes en memoria usando BytesIO
        root = ET.parse(BytesIO(xml_bytes)).getroot()
    except ET.ParseError as e:
        raise ValueError(f"XML malformado: {e}")

    lista_articulos = []

    for articulo in root.findall('PubmedArticle'):
        try:
            # ── PMID ──────────────────────────────────────────────────────────
            pmid_node = articulo.find('.//PMID')
            pmid = pmid_node.text.strip() if pmid_node is not None else "N/A"

            # ── TÍTULO ────────────────────────────────────────────────────────
            title_node = articulo.find('.//ArticleTitle')
            titulo = extraer_texto_nodo(title_node) if title_node is not None else ""
            if not titulo:
                titulo = "Sin título"

            # ── ABSTRACT ─────────────────────────────────────────────────────
            abstract_data = extraer_abstract(articulo)

            # ── TIPOS DE PUBLICACIÓN ──────────────────────────────────────────
            pub_types = [
                pt.text.strip()
                for pt in articulo.findall('.//PublicationType')
                if pt.text
            ]

            # ── NIVEL DE EVIDENCIA ────────────────────────────────────────────
            nivel_evidencia = calcular_nivel_evidencia(pub_types)

            # ── TÉRMINOS MESH ─────────────────────────────────────────────────
            mesh_tags = [
                m.text.strip()
                for m in articulo.findall('.//MeshHeading/DescriptorName')
                if m.text
            ]

            # ── KEYWORDS ─────────────────────────────────────────────────────
            keywords = [
                kw.text.strip()
                for kw in articulo.findall('.//Keyword')
                if kw.text
            ]

            # ── JOURNAL ──────────────────────────────────────────────────────
            journal_node = articulo.find('.//Journal/Title')
            journal = journal_node.text.strip() if journal_node is not None and journal_node.text else ""

            issn_node = articulo.find('.//Journal/ISSN')
            issn = issn_node.text.strip() if issn_node is not None and issn_node.text else ""

            # ── AÑO ───────────────────────────────────────────────────────────
            anio = extraer_anio(articulo)

            # ── PAÍS ──────────────────────────────────────────────────────────
            pais_node = articulo.find('.//MedlineJournalInfo/Country')
            pais = pais_node.text.strip() if pais_node is not None and pais_node.text else ""

            # ── IDIOMA ────────────────────────────────────────────────────────
            lang_nodes = articulo.findall('.//Language')
            idioma = lang_nodes[0].text.strip() if lang_nodes else "eng"

            # ── DOI ───────────────────────────────────────────────────────────
            doi = ""
            for eid in articulo.findall('.//ArticleId'):
                if eid.get('IdType') == 'doi' and eid.text:
                    doi = eid.text.strip()
                    break

            # ── TEXTO INDEXABLE ───────────────────────────────────────────────
            texto_indexable = f"{titulo} {abstract_data['abstract_full']}".strip()

            lista_articulos.append({
                "pmid":              pmid,
                "titulo":            titulo,
                "abstract_full":     abstract_data["abstract_full"],
                # abstract_sections eliminado para ahorrar espacio
                "tiene_abstract":    abstract_data["tiene_abstract"],
                "publication_types": pub_types,
                "nivel_evidencia":   nivel_evidencia,
                "mesh_tags":         mesh_tags,
                "keywords":          keywords,
                "journal":           journal,
                "issn":              issn,
                "anio":              anio,
                "pais_publicacion":  pais,
                "idioma":            idioma,
                "doi":               doi,
                "texto_indexable":   texto_indexable,
            })

        except Exception:
            continue

    return lista_articulos
