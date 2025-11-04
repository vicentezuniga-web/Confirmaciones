import pandas as pd
import streamlit as st
import zipfile
import os
from datetime import datetime

# Función para transformar el 'Tipo de Documento' basado en las reglas
def transformar_tipo(tipo, rut):
    if tipo == "FÑ":
        return "33"
    elif tipo == "FO":
        return "34"
    elif tipo == "ZV":
        if rut in ["60503000-9", "76516999-2", "9297612-2"]:
            return "34"
        else:
            return "33"
    else:
        return tipo

# Función principal para procesar el archivo
def procesar_archivo(df):
    df["Referencia"] = df["Referencia"].astype(str)
    df = df[~df["Referencia"].str.contains("-", na=False)]
    df["Referencia"] = df["Referencia"].str.rstrip(".")

    columnas_nuevas = {
        "Acreedor": "Rut emisor",
        "Clase de documento": "Tipo de Documento",
        "Referencia": "Folio",
        "Importe en moneda local": "Monto a pagar",
        "Vencimiento neto": "Fecha a pagar"
    }
    df = df.rename(columns=columnas_nuevas)

    df["Tipo de Documento"] = df.apply(
        lambda row: transformar_tipo(row["Tipo de Documento"], row["Rut emisor"]), axis=1
    )

    df["Monto a pagar"] = (
        df["Monto a pagar"].astype(str).str.replace(".", "", regex=False)
        .astype(float).abs().astype(int)
    )

    df["Fecha a pagar"] = pd.to_datetime(df["Fecha a pagar"], errors="coerce").dt.strftime("%d-%m-%Y")

    archivos_por_sociedad = {}
    for sociedad, grupo in df.groupby("Sociedad"):
        archivos_por_sociedad[sociedad] = grupo[["Rut emisor", "Tipo de Documento", "Folio", "Monto a pagar", "Fecha a pagar"]]

    return archivos_por_sociedad

# Segunda función para procesar archivos de Innova
def procesar_archivo_innova(df):
    df["Referencia"] = df["Referencia"].astype(str).str.split(".").str[0]
    

    return procesar_archivo(df)

# Configuración de la app Streamlit
st.title("Procesador de archivos de confirmación")

# Sección 1: Subir archivos estándar
st.header("Procesar archivo Saesa")
archivo_subido = st.file_uploader("Subir archivo Saesa", type=["xlsx", "xls"], key="archivo_estandar")
if archivo_subido is not None:
    try:
        df = pd.read_excel(archivo_subido)
        dfs_por_sociedad = procesar_archivo(df)
        zip_nombre = "archivos_confirmacion.zip"
        with zipfile.ZipFile(zip_nombre, "w") as zipf:
            for sociedad, df_sociedad in dfs_por_sociedad.items():
                nombre_archivo = f"Data_{sociedad}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.xlsx"
                df_sociedad.to_excel(nombre_archivo, index=False)
                zipf.write(nombre_archivo)
                os.remove(nombre_archivo)
        with open(zip_nombre, "rb") as file:
            st.download_button("Descargar ZIP", data=file, file_name=zip_nombre, mime="application/zip")
        os.remove(zip_nombre)
    except Exception as e:
        st.error(f"Error: {e}")

# Sección 2: Subir archivos de Innova
st.header("Procesar archivo de Innova")
archivo_innova = st.file_uploader("Subir archivo de Innova", type=["xlsx", "xls"], key="archivo_innova")
if archivo_innova is not None:
    try:
        df = pd.read_excel(archivo_innova)
        dfs_por_sociedad = procesar_archivo_innova(df)
        zip_nombre = "archivos_innova.zip"
        with zipfile.ZipFile(zip_nombre, "w") as zipf:
            for sociedad, df_sociedad in dfs_por_sociedad.items():
                nombre_archivo = f"Data_Innova_{sociedad}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.xlsx"
                df_sociedad.to_excel(nombre_archivo, index=False)
                zipf.write(nombre_archivo)
                os.remove(nombre_archivo)
        with open(zip_nombre, "rb") as file:
            st.download_button("Descargar ZIP", data=file, file_name=zip_nombre, mime="application/zip")
        os.remove(zip_nombre)
    except Exception as e:
        st.error(f"Error: {e}")
