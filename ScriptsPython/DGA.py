# =====================================================
# Importar librerías y cargar datos
# =====================================================
import numpy as np
import pandas as pd
import os
import json

# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/DGA.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/DGA.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/DGA.xlsx'
]

# Cargar datos
df = None
for path in addresses:
    if os.path.exists(path):
        df = pd.read_excel(path, header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")


# =====================================================
# Limpieza de datos
# =====================================================
df["SERIE"] = df["SERIE"].astype(str)
df.drop(columns=['Unnamed: 0'], inplace=True, errors='ignore')
df = df.iloc[:, :10]

# Renombrar columna de fecha
if 'FECHA DE MUESTRA' in df.columns:
    df.rename(columns={'FECHA DE MUESTRA': 'FECHA'}, inplace=True)
elif 'FECHA DE\nMUESTRA' in df.columns:
    df.rename(columns={'FECHA DE\nMUESTRA': 'FECHA'}, inplace=True)

# Procesar fecha
df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
df.dropna(subset=['FECHA'], inplace=True)

df_full = df.copy()

# =====================================================
# Lógica
# =====================================================
# Definir límites y gases
limite = {
    'H2': {'concentracion': 150, 'tasa': 132},
    'CH4': {'concentracion': 130, 'tasa': 120},
    "C2H2": {"concentracion": 20, "tasa": 4},
    "C2H4": {"concentracion": 280, "tasa": 146},
    "C2H6": {"concentracion": 90, "tasa": 90},
    "CO": {"concentracion": 600, "tasa": 1060},
    "CO2": {"concentracion": 14000, "tasa": 10000}
}
gases2 = list(limite.keys())

# Aplicar concentración
for gas in gases2:
    df[f'concentracion_{gas}'] = df[gas] > limite[gas]['concentracion']

# Calcular tasa de incremento anual
for gas in gases2:
    df[f"tasa_{gas}"] = df.groupby("SERIE").apply(
        lambda g: (g[gas].diff(-1) / ((g["FECHA"] - g["FECHA"].shift(-1)).dt.days) * 365)
    ).reset_index(level=0, drop=True)

# Aplicar flags
for gas in gases2:
    df[f"flag_{gas}"] = df[f"tasa_{gas}"] > limite[gas]['tasa']

# =====================================================
# Asignación de puntajes
# =====================================================
for gas in gases2:
    df[f'puntaje_{gas}'] = df.apply(lambda row: 
        5 if (row[f'concentracion_{gas}'] and row[f'flag_{gas}']) else
        4 if (row[f'concentracion_{gas}'] and not row[f'flag_{gas}']) else
        3 if (not row[f'concentracion_{gas}'] and row[f'flag_{gas}']) else 1, axis=1)

# =====================================================
# Cálculo de Subíndice
# =====================================================
weight = {"C2H2":5, "H2":2, "CH4":3, "C2H4":4, "C2H6":3, "CO":2, "CO2":1}
total_weight = sum(weight.values())

df['DGA'] = df.apply(
    lambda row: sum(row[f'puntaje_{gas}'] * weight[gas] for gas in gases2) / total_weight, 
    axis=1
)

df_DGA = df[['SERIE', 'FECHA', 'DGA']]

# =====================================================
# Extensión de calendario
# =====================================================
# =====================================================
# CARGAR CONFIGURACIÓN
# =====================================================
config_path = r"C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\notebooks\config.json"
with open(config_path) as f:
    config = json.load(f)

fecha_inicio = pd.Timestamp(config.get("fecha_inicio", "2015-01-01"))
print(f"✅ Fecha de inicio configurada: {fecha_inicio}")

fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

# Crear calendario completo
todas_series = df_DGA['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# Extender datos DGA
ultimos_2024 = df_DGA[df_DGA['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_DGA, ultimos_2024], ignore_index=True)

df_extendida = pd.merge(df_calendario, base_ext, on=["SERIE", "FECHA"], how="left")
df_extendida = df_extendida.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# Extender detalles
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)

df_extendida_detalles = pd.merge(df_calendario, base_ext_det, on=["SERIE", "FECHA"], how="left")
df_extendida_detalles = df_extendida_detalles.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# Combinar detalles con DGA
df_detalles = pd.merge(df_full, df_DGA, on=["SERIE", "FECHA"], how="left")
df_detalles_ext = pd.merge(df_extendida_detalles, df_extendida, on=["SERIE", "FECHA"], how="left")

# Reordenar columnas
def reordenar_columnas(df_in):
    cols = list(df_in.columns)
    if "DGA" in cols:
        cols.remove("DGA")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["DGA"] + cols[idx:]
    return df_in[cols]

df_detalles = reordenar_columnas(df_detalles)
df_detalles_ext = reordenar_columnas(df_detalles_ext)

# =====================================================
# Creación de funciones
# =====================================================
def get_df_DGA():
    return df_DGA.copy()

def get_df_extendida_DGA():
    return df_extendida.copy()

def get_df_detalles_DGA():
    return df_detalles.copy()

def get_df_detalles_ext_DGA():
    return df_detalles_ext.copy()

# =====================================================
# Resultados
# =====================================================
print('\n====== TABLA CON FECHAS ORIGINALES ======\n')
print(get_df_DGA(), '\n')

print('\n====== TABLA CON FECHAS EXTENDIDAS ======\n')
print(get_df_extendida_DGA().head(), '\n')

print('\n====== TABLA DE DETALLES DE DGA CON FECHAS ORIGINALES ======\n')
print(get_df_detalles_DGA().head(), '\n')

print('\n====== TABLA DE DETALLES DE DGA CON FECHAS EXTENDIDAS ======\n')
filtro = get_df_detalles_ext_DGA()[
    (get_df_detalles_ext_DGA()["SERIE"] == "123160T") & 
    (get_df_detalles_ext_DGA()["FECHA"] >= "2025-02-08") & 
    (get_df_detalles_ext_DGA()["FECHA"] <= "2025-02-13")
]
print(filtro.tail(10), '\n')