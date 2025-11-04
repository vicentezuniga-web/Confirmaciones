import pandas as pd
import streamlit as st
import zipfile
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo

# ---------------------------
# Configuración general
# ---------------------------
CL_TZ = ZoneInfo("America/Santiago")

REQ_COLS_BASE = {
    "Acreedor",
    "Clase de documento",
    "Referencia",
    "Importe en moneda local",
    "Vencimiento neto",
    "Sociedad",
}

# ---------------------------
# Funciones auxiliares
# ---------------------------
def validar_columnas(df: pd.DataFrame, requeridas: set):
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(faltantes)}")

def transformar_tipo(tipo: str, rut: str) -> str:
    if tipo == "FÑ":
        return "33"
    elif tipo == "FO":
        return "34"
    elif tipo == "ZV":
        if rut in {"60503000-9", "76516999-2", "9297612-2"}:
            return "34"
        else:
            return "33"
    else:
        return str(tipo)

def limpiar_folio_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.rstrip(".")
         .str.replace(r"\.0$", "", regex=True)
         .str.strip()
    )

def normalizar_monto(s: pd.Series) -> pd.Series:
    return (
        s.astype(str).str.replace(".", "", regex=False)
         .str.replace(",", ".", regex=False)  # por si viene con coma decimal
         .astype(float, errors="ignore")
    )

def formatear_fecha_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%d-%m-%Y")

def dataframes_a_zip(dfs_por_sociedad: dict, prefijo_nombre: str) -> bytes:
    """
    Crea un ZIP en memoria con 1 Excel por sociedad (o 1 archivo si es único).
    """
    zip_buffer = BytesIO()
    now_str = datetime.now(CL_TZ).strftime("%Y_%m_%d_%H_%M_%S")

    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for sociedad, df_soc in dfs_por_sociedad.items():
            excel_bytes = BytesIO()
            with pd.ExcelWriter(excel_bytes, engine="xlsxwriter") as writer:
                df_soc.to_excel(writer, index=False, sheet_name="Datos")
            excel_bytes.seek(0)
            nombre = f"{prefijo_nombre}_{sociedad}_{now_str}.xlsx"
            zf.writestr(nombre, excel_bytes.read())

    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# ---------------------------
# Procesadores existentes
# ---------------------------
def procesar_archivo(df: pd.DataFrame) -> dict:
    validar_columnas(df, REQ_COLS_BASE)

    # "Referencia" limpia (columna D origen en tus archivos Saesa)
    df["Referencia"] = df["Referencia"].astype(str)
    df = df[~df["Referencia"].str.contains("-", na=False)]
    df["Referencia"] = limpiar_folio_series(df["Referencia"])

    columnas_nuevas = {
        "Acreedor": "Rut emisor",
        "Clase de documento": "Tipo de Documento",
        "Referencia": "Folio",
        "Importe en moneda local": "Monto a pagar",
        "Vencimiento neto": "Fecha a pagar",
    }
    df = df.rename(columns=columnas_nuevas)

    df["Tipo de Documento"] = df.apply(
        lambda row: transformar_tipo(str(row["Tipo de Documento"]), str(row["Rut emisor"])),
        axis=1,
    )

    df["Monto a pagar"] = normalizar_monto(df["Monto a pagar"]).abs().astype(int, errors="ignore")
    df["Fecha a pagar"] = formatear_fecha_series(df["Fecha a pagar"])

    archivos_por_sociedad = {}
    for sociedad, grupo in df.groupby("Sociedad", dropna=False):
        sub = grupo[["Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]].copy()
        archivos_por_sociedad[str(sociedad)] = sub

    return archivos_por_sociedad

def procesar_archivo_innova(df: pd.DataFrame) -> dict:
    if "Referencia" not in df.columns:
        raise ValueError("El archivo de Innova debe contener la columna 'Referencia'.")
    df = df.copy()
    df["Referencia"] = df["Referencia"].astype(str).str.split(".").str[0]
    return procesar_archivo(df)

# ---------------------------
# NUEVO: Procesar Archivo Parauco
# ---------------------------
def procesar_archivo_parauco(df: pd.DataFrame) -> dict:
    """
    Regla:
    - Filtrar filas donde la columna L (índice 11) == 'Parque Arauco S.A.'.
    - Mapear columnas por índice:
        G (6) -> A (Rut emisor)
        D (3) -> C (Folio)
        C (2) -> D (Monto a pagar)
        E (4) -> E (Fecha a pagar)
    - Tipo de Documento = "33"
    - Si existe 'Sociedad' en el archivo, agrupar por esa columna; si no, generar un único archivo 'Parauco'.
    """
    # Validación de cantidad mínima de columnas para acceder por índice (hasta L => índice 11)
    if df.shape[1] <= 11:
        raise ValueError("El archivo no tiene suficientes columnas (se espera al menos hasta la columna L).")

    # Acceso por posición
    col_C = df.iloc[:, 2]   # C
    col_D = df.iloc[:, 3]   # D
    col_E = df.iloc[:, 4]   # E
    col_G = df.iloc[:, 6]   # G
    col_L = df.iloc[:, 11]  # L

    # Filtrar por L == "Parque Arauco S.A."
    mask = col_L.astype(str).str.strip() == "Parque Arauco S.A."
    df_f = df.loc[mask].copy()
    if df_f.empty:
        raise ValueError("No se encontraron filas con 'Parque Arauco S.A.' en la columna L.")

    # Construir DataFrame de salida con headers iguales a los otros
    out = pd.DataFrame({
        "Rut emisor": col_G.loc[mask].astype(str).str.strip(),
        "Tipo de Documento": "33",  # fijo según requerimiento de salida
        "Folio": limpiar_folio_series(col_D.loc[mask]),
        "Monto a pagar": normalizar_monto(col_C.loc[mask]),
        "Fecha a pagar": formatear_fecha_series(col_E.loc[mask]),
    })

    # Montos como enteros positivos
    out["Monto a pagar"] = pd.to_numeric(out["Monto a pagar"], errors="coerce").fillna(0).abs().astype(int)

    # Agrupar por 'Sociedad' si existe; si no, un único archivo
    archivos_por_sociedad = {}
    if "Sociedad" in df.columns:
        sociedad_vals = df.loc[mask, "Sociedad"].astype(str).fillna("SinSociedad")
        out_with_soc = out.copy()
        out_with_soc["Sociedad"] = sociedad_vals.values
        for sociedad, grupo in out_with_soc.groupby("Sociedad", dropna=False):
            sub = grupo[["Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]].copy()
            archivos_por_sociedad[str(sociedad)] = sub
    else:
        archivos_por_sociedad["Parauco"] = out

    return archivos_por_sociedad

# ---------------------------
# Interfaz Streamlit
# ---------------------------
st.set_page_config(page_title="Procesador archivos de confirmación", page_icon="📄", layout="centered")
st.title("Procesador de archivos de confirmación")
st.caption("Genera archivos por sociedad y descarga un ZIP listo para enviar.")

with st.expander("📘 Instrucciones rápidas"):
    st.markdown(
        "- **Saesa**: Debe incluir las columnas: Acreedor, Clase de documento, Referencia, Importe en moneda local, Vencimiento neto y Sociedad.\n"
        "- **Innova**: Misma estructura, pero limpia la *Referencia* antes del punto.\n"
        "- **Parauco**: Sube el Excel; se filtrará por columna **L** = 'Parque Arauco S.A.' y se mapearán columnas G→A (Rut), D→C (Folio), C→D (Monto), E→E (Fecha).\n"
        "- El ZIP contiene 1 Excel por **Sociedad** cuando aplique.\n"
        "- Fechas en **dd-mm-YYYY**, montos enteros positivos, y folios sin sufijo `.0`."
    )

# --- Sección Saesa ---
st.header("Procesar archivo Saesa")
archivo_saesa = st.file_uploader("Sube archivo Saesa (.xlsx / .xls)", type=["xlsx", "xls"], key="saesa")

if archivo_saesa is not None:
    try:
        df_saesa = pd.read_excel(archivo_saesa)
        dfs_soc_saesa = procesar_archivo(df_saesa)
        zip_bytes = dataframes_a_zip(dfs_soc_saesa, "Data")
        st.download_button(
            label="📦 Descargar ZIP Saesa",
            data=zip_bytes,
            file_name="archivos_confirmacion_saesa.zip",
            mime="application/zip",
        )
        st.success(f"Listo ✅ Se generaron {len(dfs_soc_saesa)} archivo(s) por sociedad.")
    except Exception as e:
        st.error(f"Error procesando Saesa: {e}")

# --- Sección Innova ---
st.header("Procesar archivo Innova")
archivo_innova = st.file_uploader("Sube archivo Innova (.xlsx / .xls)", type=["xlsx", "xls"], key="innova")

if archivo_innova is not None:
    try:
        df_innova = pd.read_excel(archivo_innova)
        dfs_soc_innova = procesar_archivo_innova(df_innova)
        zip_bytes = dataframes_a_zip(dfs_soc_innova, "Data_Innova")
        st.download_button(
            label="📦 Descargar ZIP Innova",
            data=zip_bytes,
            file_name="archivos_confirmacion_innova.zip",
            mime="application/zip",
        )
        st.success(f"Listo ✅ Se generaron {len(dfs_soc_innova)} archivo(s) por sociedad.")
    except Exception as e:
        st.error(f"Error procesando Innova: {e}")

# --- NUEVA Sección Parauco ---
st.header("Procesar Archivo Parauco")
archivo_parauco = st.file_uploader("Sube archivo Parauco (.xlsx / .xls)", type=["xlsx", "xls"], key="parauco")

if archivo_parauco is not None:
    try:
        df_parauco = pd.read_excel(archivo_parauco, header=0)
        dfs_soc_parauco = procesar_archivo_parauco(df_parauco)
        zip_bytes = dataframes_a_zip(dfs_soc_parauco, "Data_Parauco")
        st.download_button(
            label="📦 Descargar ZIP Parauco",
            data=zip_bytes,
            file_name="archivos_confirmacion_parauco.zip",
            mime="application/zip",
        )
        st.success(f"Listo ✅ Se generaron {len(dfs_soc_parauco)} archivo(s).")
    except Exception as e:
        st.error(f"Error procesando Parauco: {e}")
