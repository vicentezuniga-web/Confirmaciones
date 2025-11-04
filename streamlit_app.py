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
    Cr
