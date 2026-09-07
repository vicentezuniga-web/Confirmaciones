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
    "Proveedor",
    "Clase de documento",
    "Referencia",
    "Importe en moneda local",
    "Vencimiento neto",
    "Sociedad",
}

SOCIEDADES_PARAUCO = {
    "Arauco Centros Comerciales Regionales SPA",
    "Arauco Chillán SPA",
    "Arauco Malls Chile S.A.",
    "Bulevar Rentas Inmobiliarias S.A.",
    "Centros Comerciales Vecinales Arauco Express S.A.",
    "Desarrollos Inmobiliarios San Antonio S.A.",
    "Inmob. Paseo Estacion",
    "Inversiones Arauco Spa.",
    "Parque Angamos SPA",
    "Parque Arauco S.A.",
    "Plaza Estación S.A.",
    "Todo Arauco S.A.",
}

# SAESA: letra sociedad -> RUT
SAESA_SOCIEDAD_A_RUT = {
    "D": "76519747-3",
    "E": "88272600-2",
    "F": "76073164-1",
    "G": "77708654-5",
    "L": "96531500-4",
    "S": "76073162-5",
    "T": "77312201-6",
}

# INNOVA: letra sociedad -> RUT
INNOVA_SOCIEDAD_A_RUT = {
    "P": "77227565-K",
}

# ---------------------------
# Helpers
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
         .str.replace(",", ".", regex=False)
         .str.strip()
    )

def formatear_fecha_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%d-%m-%Y")

def normalizar_rut(valor) -> str:
    """
    76.939.541-5 -> 76939541-5
    ' 76073164 - 1 ' -> 76073164-1
    """
    s = "" if pd.isna(valor) else str(valor)
    s = s.strip()
    s = s.replace(".", "")
    s = s.replace(" ", "")
    s = s.upper()
    return s

def dataframes_a_zip(dfs_por_sociedad: dict, prefijo_nombre: str) -> bytes:
    """Crea un ZIP en memoria con 1 Excel por sociedad."""
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

def dataframe_a_excel_bytes(df: pd.DataFrame, sheet_name: str = "Datos") -> bytes:
    excel_bytes = BytesIO()
    with pd.ExcelWriter(excel_bytes, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    excel_bytes.seek(0)
    return excel_bytes.getvalue()

def base_a_dict_por_sociedad(base: pd.DataFrame, col_sociedad: str = "Sociedad") -> dict:
    """Convierte base a dict{sociedad: df_salida_sin_col_sociedad}"""
    out = {}
    for sociedad, grupo in base.groupby(col_sociedad, dropna=False):
        sub = grupo[["Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]].copy()
        if not sub.empty:
            out[str(sociedad)] = sub
    if not out:
        raise ValueError("No se generaron archivos por sociedad: no hay grupos con filas válidas.")
    return out

def preparar_unificado(df_base: pd.DataFrame) -> pd.DataFrame:
    """
    Para descargas UNIFICADAS:
    - Cambia el header 'Sociedad' a 'Rut pagadora'
    - Mantiene el resto igual
    """
    out = df_base.rename(columns={"Sociedad": "Rut pagadora"}).copy()
    # asegura orden (rut pagadora primero)
    cols = ["Rut pagadora", "Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]
    return out[cols]

# ---------------------------
# Builder SAESA-like base (sin mapping)
# ---------------------------
def construir_base_saesa_like_sin_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve DF estandarizado con columnas:
    Sociedad, Rut emisor, Tipo de Documento, Folio, Monto a pagar, Fecha a pagar
    SIN aplicar reemplazo de Sociedad.
    """
    validar_columnas(df, REQ_COLS_BASE)
    df = df.copy()

    # Excluir filas sin referencia
    ref = df["Referencia"]
    mask_ref_valida = (
        ref.notna()
        & ref.astype(str).str.strip().ne("")
        & ref.astype(str).str.strip().str.lower().ne("nan")
    )
    df = df.loc[mask_ref_valida].copy()
    if df.empty:
        raise ValueError("No hay filas válidas: todas las filas tienen 'Referencia' vacía o inválida.")

    df["Referencia"] = df["Referencia"].astype(str)
    df = df[~df["Referencia"].str.contains("-", na=False)]
    df["Referencia"] = limpiar_folio_series(df["Referencia"])
    if df.empty:
        raise ValueError("No hay filas válidas después de filtrar referencias con '-'.")

    columnas_nuevas = {
        "Proveedor": "Rut emisor",
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

    df["Monto a pagar"] = pd.to_numeric(
        normalizar_monto(df["Monto a pagar"]),
        errors="coerce"
    ).fillna(0).abs().astype(int)

    df["Fecha a pagar"] = formatear_fecha_series(df["Fecha a pagar"])

    out = df[["Sociedad", "Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]].copy()

    out = out[
        out["Rut emisor"].astype(str).str.strip().ne("")
        & out["Folio"].astype(str).str.strip().ne("")
    ].copy()

    if out.empty:
        raise ValueError("No se generó salida: quedó vacío tras filtros/limpieza.")
    return out

# ---------------------------
# SAESA / INNOVA processors
# ---------------------------
def construir_base_saesa(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Limpieza específica SAESA: eliminar caracteres NO numéricos de la columna 'Referencia' (col. D)
    # Ej.: "FA-12345" -> "12345"; "123/A" -> "123"; "ABC" -> "" (la fila se descartará luego por mask_ref_valida)
    # El decimal se quita ANTES: si la columna se lee como float, "12345.0" perdería el punto
    # y quedaría "123450" (un cero de más al final del folio).
    if "Referencia" in df.columns:
        df["Referencia"] = (
            df["Referencia"]
            .astype(str)
            .str.replace(r"\.\d+$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
        )
    base = construir_base_saesa_like_sin_mapping(df)
    # Sociedad letra -> RUT; inválidos se eliminan
    base["Sociedad"] = base["Sociedad"].astype(str).str.strip().str.upper()
    base["Sociedad"] = base["Sociedad"].map(SAESA_SOCIEDAD_A_RUT)
    base = base[base["Sociedad"].notna()].copy()
    base["Sociedad"] = base["Sociedad"].apply(normalizar_rut)
    if base.empty:
        raise ValueError("SAESA: no quedaron filas válidas luego de aplicar el mapping de sociedades.")
    return base

def construir_base_innova(df: pd.DataFrame) -> pd.DataFrame:
    if "Referencia" not in df.columns:
        raise ValueError("El archivo de Innova debe contener la columna 'Referencia'.")
    df = df.copy()
    df["Referencia"] = df["Referencia"].astype(str).str.split(".").str[0]

    base = construir_base_saesa_like_sin_mapping(df)

    # Sociedad letra -> RUT (solo P); inválidos se eliminan
    base["Sociedad"] = base["Sociedad"].astype(str).str.strip().str.upper()
    base["Sociedad"] = base["Sociedad"].map(INNOVA_SOCIEDAD_A_RUT)
    base = base[base["Sociedad"].notna()].copy()
    base["Sociedad"] = base["Sociedad"].apply(normalizar_rut)

    if base.empty:
        raise ValueError("INNOVA: no quedaron filas válidas (solo se acepta Sociedad='P').")
    return base

# ---------------------------
# PARQUE ARAUCO processors
# ---------------------------
def construir_base_parauco(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Filtra por sociedades válidas en columna L (nombre).
    - 'Sociedad' de salida viene desde columna K (RUT sociedad) y se normaliza (sin puntos).
    """
    if df.shape[1] <= 11:
        raise ValueError("El archivo no tiene suficientes columnas (se espera al menos hasta la columna L).")

    col_C = df.iloc[:, 2]    # C -> Monto a pagar
    col_D = df.iloc[:, 3]    # D -> Folio
    col_E = df.iloc[:, 4]    # E -> Fecha a pagar
    col_G = df.iloc[:, 6]    # G -> Rut emisor
    col_K = df.iloc[:, 10]   # K -> RUT sociedad
    col_L = df.iloc[:, 11]   # L -> Nombre sociedad (para filtrar)

    col_L_norm = col_L.astype(str).str.strip()
    mask = col_L_norm.isin(SOCIEDADES_PARAUCO)

    if not mask.any():
        raise ValueError("No se encontraron filas con sociedades Parauco válidas en la columna L.")

    sociedad_rut = col_K.loc[mask].apply(normalizar_rut)

    out = pd.DataFrame({
        "Sociedad": sociedad_rut.values,
        "Rut emisor": col_G.loc[mask].astype(str).str.strip(),
        "Tipo de Documento": "33",
        "Folio": limpiar_folio_series(col_D.loc[mask]),
        "Monto a pagar": pd.to_numeric(normalizar_monto(col_C.loc[mask]), errors="coerce"),
        "Fecha a pagar": formatear_fecha_series(col_E.loc[mask]),
    })

    out["Monto a pagar"] = out["Monto a pagar"].fillna(0).abs().astype(int)

    out = out[
        out["Sociedad"].astype(str).str.strip().ne("")
        & out["Rut emisor"].astype(str).str.strip().ne("")
        & out["Folio"].astype(str).str.strip().ne("")
    ].copy()

    if out.empty:
        raise ValueError("Parauco: quedó vacío tras limpieza/filtros.")
    return out

# ---------------------------
# UI Streamlit
# ---------------------------
st.set_page_config(page_title="Procesador archivos de confirmación", page_icon="📄", layout="centered")
st.title("Procesador de archivos de confirmación")
st.caption("Descarga en modo unificado (1 Excel) o por sociedad (ZIP).")

with st.expander("📘 Instrucciones rápidas"):
    st.markdown(
        "- **Saesa**: Sociedad viene como letra (D/E/F/G/L/S/T) y se reemplaza por su **RUT pagadora**. Si no coincide, se elimina la fila.\n"
        "- **Innova**: Solo se acepta Sociedad = P y se reemplaza por **RUT pagadora** 77227565-K. El resto se elimina.\n"
        "- **Parauco**: Se filtra por nombre sociedad en L (lista), pero la **Rut pagadora** del output se toma de K (RUT) y se limpia (sin puntos).\n"
        "- **Unificado**: 1 Excel con header **Rut pagadora**.\n"
        "- **Por sociedad**: ZIP con 1 Excel por Rut pagadora."
    )

now_str = datetime.now(CL_TZ).strftime("%Y_%m_%d_%H_%M_%S")

# --- SAESA ---
st.header("SAESA")
modo_saesa = st.radio(
    "Modo de salida SAESA",
    ["Unificado (1 Excel)", "Por sociedad (ZIP)"],
    horizontal=True,
    key="modo_saesa",
)
archivo_saesa = st.file_uploader("Sube archivo SAESA (.xlsx / .xls)", type=["xlsx", "xls"], key="saesa")

if archivo_saesa is not None:
    try:
        df_saesa = pd.read_excel(archivo_saesa)
        base_saesa = construir_base_saesa(df_saesa)

        if modo_saesa.startswith("Unificado"):
            unificado_saesa = preparar_unificado(base_saesa)
            excel_bytes = dataframe_a_excel_bytes(unificado_saesa)
            st.download_button(
                label="⬇️ Descargar SAESA unificado",
                data=excel_bytes,
                file_name=f"confirmacion_saesa_unificado_{now_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.success(f"Listo ✅ Unificado SAESA: {len(unificado_saesa)} filas.")
        else:
            dict_saesa = base_a_dict_por_sociedad(base_saesa, col_sociedad="Sociedad")
            zip_bytes = dataframes_a_zip(dict_saesa, "Data_SAESA")
            st.download_button(
                label="📦 Descargar ZIP SAESA por sociedad",
                data=zip_bytes,
                file_name=f"confirmacion_saesa_por_sociedad_{now_str}.zip",
                mime="application/zip",
            )
            st.success(f"Listo ✅ ZIP SAESA: {len(dict_saesa)} archivo(s) por sociedad.")
    except Exception as e:
        st.error(f"Error procesando SAESA: {e}")

# --- INNOVA ---
st.header("INNOVA")
modo_innova = st.radio(
    "Modo de salida INNOVA",
    ["Unificado (1 Excel)", "Por sociedad (ZIP)"],
    horizontal=True,
    key="modo_innova",
)
archivo_innova = st.file_uploader("Sube archivo INNOVA (.xlsx / .xls)", type=["xlsx", "xls"], key="innova")

if archivo_innova is not None:
    try:
        df_innova = pd.read_excel(archivo_innova)
        base_innova = construir_base_innova(df_innova)

        if modo_innova.startswith("Unificado"):
            unificado_innova = preparar_unificado(base_innova)
            excel_bytes = dataframe_a_excel_bytes(unificado_innova)
            st.download_button(
                label="⬇️ Descargar INNOVA unificado",
                data=excel_bytes,
                file_name=f"confirmacion_innova_unificado_{now_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.success(f"Listo ✅ Unificado INNOVA: {len(unificado_innova)} filas.")
        else:
            dict_innova = base_a_dict_por_sociedad(base_innova, col_sociedad="Sociedad")
            zip_bytes = dataframes_a_zip(dict_innova, "Data_INNOVA")
            st.download_button(
                label="📦 Descargar ZIP INNOVA por sociedad",
                data=zip_bytes,
                file_name=f"confirmacion_innova_por_sociedad_{now_str}.zip",
                mime="application/zip",
            )
            st.success(f"Listo ✅ ZIP INNOVA: {len(dict_innova)} archivo(s) por sociedad.")
    except Exception as e:
        st.error(f"Error procesando INNOVA: {e}")

# --- PARQUE ARAUCO ---
st.header("PARQUE ARAUCO")
modo_parauco = st.radio(
    "Modo de salida Parauco",
    ["Unificado (1 Excel)", "Por sociedad (ZIP)"],
    horizontal=True,
    key="modo_parauco",
)
archivo_parauco = st.file_uploader("Sube archivo PARQUE ARAUCO (.xlsx / .xls)", type=["xlsx", "xls"], key="parauco")

if archivo_parauco is not None:
    try:
        df_parauco = pd.read_excel(archivo_parauco, header=0)
        base_parauco = construir_base_parauco(df_parauco)

        if modo_parauco.startswith("Unificado"):
            unificado_parauco = preparar_unificado(base_parauco)
            excel_bytes = dataframe_a_excel_bytes(unificado_parauco)
            st.download_button(
                label="⬇️ Descargar Parauco unificado",
                data=excel_bytes,
                file_name=f"confirmacion_parauco_unificado_{now_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.success(f"Listo ✅ Unificado Parauco: {len(unificado_parauco)} filas.")
        else:
            dict_parauco = base_a_dict_por_sociedad(base_parauco, col_sociedad="Sociedad")
            zip_bytes = dataframes_a_zip(dict_parauco, "Data_PARAUCO")
            st.download_button(
                label="📦 Descargar ZIP Parauco por sociedad",
                data=zip_bytes,
                file_name=f"confirmacion_parauco_por_sociedad_{now_str}.zip",
                mime="application/zip",
            )
            st.success(f"Listo ✅ ZIP Parauco: {len(dict_parauco)} archivo(s) por sociedad.")
    except Exception as e:
        st.error(f"Error procesando Parauco: {e}")
