import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RNUCLEO.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RNUCLEO.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/RNUCLEO.xlsx'
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
df = df.drop(columns=['Unnamed: 0'])
df =df.rename(columns={'FECHA DE MUESTRA':'FECHA'})  # Elimina espacios en los nombres de las columnas
df

# Asignar puntaje RNUC según la tabla NETA ATS 2021
# Suponiendo que la columna de valores se llama 'VALOR' (ajusta si es diferente)

col = [c for c in df.columns if c not in ['SERIE', 'FECHA']][0]
df['RNUC'] = np.where(df[col] < 500, 5, 1)
df = df[['SERIE','FECHA','RNUC','Valor (MΩ)']]
print(df)

# =============================
# TABLAS FINALES Y FUNCIONES
# =============================
# 1. Tabla con fechas originales: SERIE, FECHA, RNUC
cols_base = [col for col in df.columns if col in ['SERIE', 'FECHA', 'RNUC']]
df_RNUC = df[cols_base].copy()

# 2. Tabla con fechas extendidas: SERIE, FECHA, RNUC
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

ultimos_2014 = df_RNUC[df_RNUC['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014['FECHA'] = fecha_inicio
base_ext = pd.concat([df_RNUC, ultimos_2014], ignore_index=True)
df_RNUC_ext = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_RNUC_ext = df_RNUC_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# 3. Tabla de detalles con fechas originales
cols_detalles = [col for col in df.columns if col not in []]  # todas las columnas originales
cols_detalles = ['SERIE', 'FECHA', 'RNUC'] + [col for col in cols_detalles if col not in ['SERIE', 'FECHA', 'RNUC']]
df_detalles = df[cols_detalles].copy()

# 4. Tabla de detalles con fechas extendidas
ultimos_2014_det = df_detalles[df_detalles['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_detalles, ultimos_2014_det], ignore_index=True)
df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# =============================
# FUNCIONES PARA LLAMAR
# =============================
def get_df_RNUC():
    return df_RNUC

def get_df_extendida_RNUC():
    return df_RNUC_ext

def get_df_detalles_RNUC():
    return df_detalles

def get_df_detalles_ext_RNUC():
    return df_detalles_ext

# =============================
# PRINT DE TABLAS
# =============================
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_RNUC(), '\n')
print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_RNUC().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_RNUC().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_RNUC().head(), '\n')