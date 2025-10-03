import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FQOLTC.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FQOLTC.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/FQOLTC.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):   # verifica si existe
        df = pd.read_excel(path,header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")
df = df.drop(columns=df.columns[0])
df = df.rename(columns={'Valor RD (2mm gap) (kV/2mm)': 'RD','Valor H2O (ppm)':'H20'})

# Puntaje para RD
df['Puntaje_RD'] = np.where(df['RD'] < 30, 5, 1)

# Puntaje para H2O
df['Puntaje_H2O'] = np.where(df['H20'] > 30, 5, 1)


# Calcular columna OLTC ponderado con los puntajes
# OLTC = (5 * Puntaje_RD + 3 * Puntaje_H2O) / 8
df['OLTC'] = (5 * df['Puntaje_RD'] + 3 * df['Puntaje_H2O']) / 8

# Filtrar y reordenar columnas
columnas_orden = ['SERIE', 'FECHA DE MUESTRA', 'OLTC', 'RD', 'H20']
df_full  = df[columnas_orden]
# 1. Tabla con fechas originales
df_OLTC = df[['SERIE', 'FECHA DE MUESTRA', 'OLTC']].copy()

# 2. Tabla con fechas extendidas
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE","FECHA DE MUESTRA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

ultimos_2014 = df_OLTC[df_OLTC['FECHA DE MUESTRA'] < fecha_inicio].sort_values('FECHA DE MUESTRA').groupby('SERIE').tail(1)
ultimos_2014['FECHA DE MUESTRA'] = fecha_inicio
base_ext = pd.concat([df_OLTC, ultimos_2014], ignore_index=True)
df_OLTC_ext = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA DE MUESTRA"], how="left")
df_OLTC_ext = df_OLTC_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

def get_df_extendida_OLTC():
    return df_OLTC_ext

# 3. Tabla de detalles con fechas originales
df_detalles = df[['SERIE', 'FECHA DE MUESTRA', 'OLTC', 'RD', 'H20']].copy()

# 4. Tabla de detalles con fechas extendidas
ultimos_2014_det = df_detalles[df_detalles['FECHA DE MUESTRA'] < fecha_inicio].sort_values('FECHA DE MUESTRA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA DE MUESTRA'] = fecha_inicio
base_ext_det = pd.concat([df_detalles, ultimos_2014_det], ignore_index=True)
df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA DE MUESTRA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# Mostrar las tablas
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(df_OLTC.head(), '\n')

print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(df_OLTC_ext.head(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(df_detalles.head(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(df_detalles_ext, '\n')