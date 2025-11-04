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
        # Reglas especiales por RUT
        if rut in {"60503000-9", "76516999-2", "9297612-2"}:
            return "34"
        else:
            return "33"
    else:
        return str(tipo)

def procesar_archivo(df: pd.DataFrame) -> dict:
    validar_columnas(df, REQ_COLS_BASE)

    # --- Limpieza y normalización de "Referencia" (columna D original) ---
    df["Referencia"] = df["Referencia"].astype(str)

    # Eliminar filas con guiones
    df = df[~df["Referencia"].str.contains("-", na=False)]

    # Quitar puntos finales y eliminar el ".0" de números (FIX solicitado)
    df["Referencia"] = (
        df["Referencia"]
        .str.rstrip(".")
        .str.replace(r"\.0$", "", regex=True)  # ✅ elimina el .0 al final
        .str.strip()
    )

    # --- Renombrar columnas a las esperadas en la salida ---
    columnas_nuevas = {
        "Acreedor": "Rut emisor",
        "Clase de documento": "Tipo de Documento",
        "Referencia": "Folio",
        "Importe en moneda local": "Monto a pagar",
        "Vencimiento neto": "Fecha a pagar",
    }
    df = df.rename(columns=columnas_nuevas)

    # --- Transformar Tipo de Documento según reglas ---
    df["Tipo de Documento"] = df.apply(
        lambda row: transformar_tipo(str(row["Tipo de Documento"]), str(row["Rut emisor"])),
        axis=1,
    )

    # --- Monto a pagar (formato entero positivo sin puntos) ---
    df["Monto a pagar"] = (
        df["Monto a pagar"]
        .astype(str).str.replace(".", "", regex=False)
        .astype(float).abs().astype(int)
    )

    # --- Fecha a pagar en formato dd-mm-YYYY ---
    df["Fecha a pagar"] = pd.to_datetime(df["Fecha a pagar"], errors="coerce").dt.strftime("%d-%m-%Y")

    # --- Agrupar por Sociedad ---
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

def dataframes_a_zip(dfs_por_sociedad: dict, prefijo_nombre: str) -> bytes:
    """
    Crea un ZIP en memoria con 1 Excel por sociedad.
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
# Interfaz Streamlit
# ---------------------------
st.set_page_config(page_title="Procesador archivos de confirmación", page_icon="📄", layout="centered")
st.title("Procesador de archivos de confirmación")
st.caption("Genera archivos por sociedad y descarga un ZIP listo para enviar.")

with st.expander("📘 Instrucciones rápidas"):
    st.markdown(
        "- **Saesa**: Debe incluir las columnas: Acreedor, Clase de documento, Referencia, Importe en moneda local, Vencimiento neto y Sociedad.\n"
        "- **Innova**: Misma estructura, pero limpia la *Referencia* antes del punto.\n"
        "- El ZIP contiene 1 Excel por **Sociedad**.\n"
        "- Las fechas se formatean a **dd-mm-YYYY** y los montos quedan como enteros positivos.\n"
        "- Se eliminan los `.0` al final de los folios numéricos."
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
