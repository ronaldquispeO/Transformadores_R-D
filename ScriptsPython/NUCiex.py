import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx'
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
# df = df.drop(columns=['Unnamed: 0'])
df = df.rename(columns ={'FECHA DE MUESTRA':'FECHA'})
df = df[['SERIE', 'FECHA'] + [col for col in df.columns if col.endswith("mA")]] # seleccionar columnas relevantes


# Calcular columnas delta respecto al valor inicial por SERIE en la fecha mínima
# Detectar columnas numéricas a procesar (excluyendo SERIE y FECHA)
cols_val = [c for c in df.columns if c not in ['SERIE', 'FECHA'] and np.issubdtype(df[c].dtype, np.number)]

for col in cols_val:
    ref = df.groupby('SERIE').apply(lambda g: g.loc[g['FECHA'].idxmin(), col])
    ref_mapped = df['SERIE'].map(ref)
    delta = abs((df[col] - ref_mapped) / ref_mapped) * 100
    delta = delta.replace([np.inf, -np.inf], np.nan)
    mask = df[col].isna() | ref_mapped.isna() | (ref_mapped == 0) | (df[col] == 0)
    delta[mask] = np.nan
    df[f'{col}_Delta'] = delta
    
# Calcular el máximo de las columnas que terminan en 'Delta' para cada fila
cols_delta = [col for col in df.columns if col.endswith('Delta')]
df['Max_Iex'] = df[cols_delta].max(axis=1)

# Asignar puntaje IEX según la tabla SD MYERS
# Si Max_Iex > 10% → 5, si Max_Iex ≤ 10% → 1
df['IEX'] = np.where(df['Max_Iex'] > 10, 5, 1)

# Crear tabla final solo con SERIE, FECHA, IEX y las columnas que no terminan en 'Delta' ni son 'Max_Iex'
cols_final = [col for col in df.columns if not col.endswith('Delta') and col != 'Max_Iex']
# Aseguramos el orden: SERIE, FECHA, IEX primero
cols_final = ['SERIE', 'FECHA', 'IEX'] + [col for col in cols_final if col not in ['SERIE', 'FECHA', 'IEX']]
df_final = df[cols_final]

# =============================
# TABLAS FINALES Y FUNCIONES
# =============================
# 1. Tabla con fechas originales: SERIE, FECHA, IEX
cols_base = [col for col in df_final.columns if col in ['SERIE', 'FECHA', 'IEX']]
df_IEX = df_final[cols_base].copy()

# 2. Tabla con fechas extendidas: SERIE, FECHA, IEX
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

ultimos_2014 = df_IEX[df_IEX['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014['FECHA'] = fecha_inicio
base_ext = pd.concat([df_IEX, ultimos_2014], ignore_index=True)
df_IEX_ext = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_IEX_ext = df_IEX_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# 3. Tabla de detalles con fechas originales
cols_detalles = [col for col in df_final.columns]
cols_detalles = ['SERIE', 'FECHA', 'IEX'] + [col for col in cols_detalles if col not in ['SERIE', 'FECHA', 'IEX']]
df_detalles = df_final[cols_detalles].copy()

# 4. Tabla de detalles con fechas extendidas
ultimos_2014_det = df_detalles[df_detalles['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_detalles, ultimos_2014_det], ignore_index=True)
df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# =============================
# FUNCIONES PARA LLAMAR
# =============================
def get_df_IEX():
    return df_IEX

def get_df_extendida_IEX():
    return df_IEX_ext

def get_df_detalles_IEX():
    return df_detalles

def get_df_detalles_ext_IEX():
    return df_detalles_ext

# =============================
# PRINT DE TABLAS
# =============================
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_IEX().head(20), '\n')
print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_IEX().tail(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_IEX().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_IEX().head(), '\n')