import numpy as np
import pandas as pd
import os

# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/ECC.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/ECC.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/ECC.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):   # verifica si existe
        df = pd.read_excel(path,header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")
df["SERIE"] = df["SERIE"].astype(str)
df['SERIE'] = df['SERIE'].astype(str).str.replace(" ", "")

# ---------------------------
# LIMPIEZA DE DATOS
# ---------------------------
df = df.drop(columns=['Unnamed: 0'])
# df["TENSION"] = df["TENSION"].str.split("/").str[0]
df = df.iloc[:, :10]
if 'FECHA DE MUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE MUESTRA': 'FECHA'})
elif 'FECHA DE\nMUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE\nMUESTRA': 'FECHA'})
df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
df = df.dropna(subset=['FECHA'])
df["SERIE"] = df["SERIE"].astype(str)
# Guardar tabla original (solo fechas de medición)
# df_full = df.drop(columns=["TENSION"]).copy()
df_full = df.copy()
print(df)

# ---------------------------
# LÓGICA ECC - ASIGNAR PUNTAJE (CORREGIDA)
# ---------------------------

def asignar_puntaje_ecc(valor_ecc):
    """
    Asigna puntaje según la lógica de Energía Acumulada
    """
    # SI es NaN, devolver NaN
    if pd.isna(valor_ecc):
        return np.nan
    
    if valor_ecc > 4:
        return 5
    elif 3 < valor_ecc <= 4:
        return 4
    elif 2 < valor_ecc <= 3:
        return 3
    elif 1 < valor_ecc <= 2:
        return 2
    else:  # ECC ≤ 1
        return 1

# Aplicar la lógica ECC
df['puntaje_ECC'] = df['EAC'].apply(asignar_puntaje_ecc)

# Crear tabla ECC (similar a DGA) - SOLO filas con valores reales
df_ECC = df[['SERIE', 'FECHA', 'puntaje_ECC']].copy()
df_ECC = df_ECC.rename(columns={'puntaje_ECC': 'ECC'})

# ---------------------------
# EXTENSIÓN DEL CALENDARIO (MISMA LÓGICA QUE DGA)
# ---------------------------

inicio = "2015-01-01"
desde_2025 = f"{pd.Timestamp.today().year}-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

# Calendario
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE","FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- Tabla extendida de ECC ----------
ultimos_2024 = df_ECC[df_ECC['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_ECC, ultimos_2024], ignore_index=True)

df_extendida = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida = df_extendida.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla extendida de detalles ----------
# SOLO unir sin ffill para mantener NaN donde no hay datos
df_extendida_detalles = pd.merge(df_calendario, df_full, on=["SERIE","FECHA"], how="left")

# ---------------------------
# DETALLES + ECC (CORREGIDO)
# ---------------------------
# Para detalles originales: unir normalmente
df_detalles = pd.merge(df_full, df_ECC, on=["SERIE","FECHA"], how="left")

# Para detalles extendidos: unir extendida_detalles (con NaN) con extendida_ECC
df_detalles_ext = pd.merge(df_extendida_detalles, df_extendida, on=["SERIE","FECHA"], how="left")

# Reordenar columnas: poner ECC después de FECHA
def reordenar(df_in):
    cols = list(df_in.columns)
    if "ECC" in cols:
        cols.remove("ECC")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["ECC"] + cols[idx:]
    return df_in[cols]

df_detalles = reordenar(df_detalles)
df_detalles_ext = reordenar(df_detalles_ext)

# ---------------------------
# FUNCIONES PARA LLAMAR (MISMA ESTRUCTURA QUE DGA)
# ---------------------------
def get_df_ECC():
    return df_ECC

def get_df_extendida_ECC():
    return df_extendida

def get_df_detalles_ECC():
    return df_detalles

def get_df_detalles_ext_ECC():
    return df_detalles_ext

# ---------------------------
# MOSTRAR RESULTADOS
# ---------------------------
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_ECC(), '\n')

print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_ECC().tail(), '\n')

print('\n ====== TABLA DE DETALLES DE ECC CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_ECC().head(), '\n')

print('\n ====== TABLA DE DETALLES DE ECC CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_ECC()[get_df_detalles_ext_ECC()["SERIE"]=="D518293"].tail(10), '\n')