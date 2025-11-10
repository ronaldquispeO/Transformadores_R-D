import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RDIS.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RDIS.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/RDIS.xlsx'
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
df['SERIE'] = df['SERIE'].astype(str).str.replace(" ", "").str.upper()
# df = df.drop(columns=df.columns[0])
df = df[['SERIE', 'FECHA'] + [col for col in df.columns if col.endswith(('[%]', '2'))]] # seleccionar columnas relevantes
# Aseguramos que FECHA sea datetime
df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
# df["SERIE"] = df["SERIE"].astype(str)

cap_cols = df.iloc[:,2:]

for col in cap_cols:
    # valor inicial por SERIE en la fecha mínima
    ref = df.groupby("SERIE").apply(lambda g: g.loc[g["FECHA"].idxmin(), col])
    ref_mapped = df["SERIE"].map(ref)
     # variación porcentual
    delta = abs((df[col] - ref_mapped) / ref_mapped) * 100
    delta = delta.replace([np.inf, -np.inf], np.nan)

    # marcar casos inválidos
    mask = df[col].isna() | ref_mapped.isna() | (ref_mapped == 0) | (df[col] == 0)
    delta[mask] = np.nan

    # agregar columna nueva
    df[f"{col}_Delta"] = delta


# Calcular el máximo de las columnas que terminan en 'Delta' para cada fila
cols_delta = [col for col in df.columns if col.endswith('Delta')]
df['Max_Dis'] = df[cols_delta].max(axis=1)
df

# Asignar puntaje RDIS según el valor de Max_Dis y la tabla IEEE C57.152-2013
conditions = [
    df['Max_Dis'] > 3,
    (df['Max_Dis'] > 2) & (df['Max_Dis'] <= 3),
    df['Max_Dis'] <= 2
]
choices = [5, 3, 1]
df['RDIS'] = np.select(conditions, choices, default=np.nan)


# ---------------------------
# TABLAS FINALES Y FUNCIONES
# ---------------------------
# 1. Tabla con fechas originales: SERIE, FECHA, RDIS
df_DIS = df[['SERIE', 'FECHA', 'RDIS']].copy()

# 2. Tabla con fechas extendidas: SERIE, FECHA, RDIS
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

ultimos_2014 = df_DIS[df_DIS['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014['FECHA'] = fecha_inicio
base_ext = pd.concat([df_DIS, ultimos_2014], ignore_index=True)
df_DIS_ext = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_DIS_ext = df_DIS_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# 3. Tabla de detalles con fechas originales
cols_final = [col for col in df.columns if not col.endswith('Delta') and col != 'Max_Dis']
cols_final = ['SERIE', 'FECHA', 'RDIS'] + [col for col in cols_final if col not in ['SERIE', 'FECHA', 'RDIS']]
df_detalles = df[cols_final].copy()

# 4. Tabla de detalles con fechas extendidas
ultimos_2014_det = df_detalles[df_detalles['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_detalles, ultimos_2014_det], ignore_index=True)
df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_DIS():
    return df_DIS

def get_df_extendida_DIS():
    return df_DIS_ext

def get_df_detalles_DIS():
    return df_detalles

def get_df_detalles_ext_DIS():
    return df_detalles_ext

# ---------------------------
# PRINT DE TABLAS
# ---------------------------
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_DIS().head(20), '\n')
print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_DIS().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_DIS().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_DIS().head(), '\n')