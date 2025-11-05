import numpy as np
import pandas as pd
import os

# ---------------------------
# CARGA DEL ARCHIVO OLTC
# ---------------------------
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FQOLTC.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FQOLTC.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/FQOLTC.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):
        df = pd.read_excel(path, header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")
df["SERIE"] = df["SERIE"].astype(str)
df['SERIE'] = df['SERIE'].astype(str).str.replace(" ", "").str.upper()
# ---------------------------
# LIMPIEZA DE DATOS
# ---------------------------
df["SERIE"] = df["SERIE"].astype(str)
df = df.drop(columns=df.columns[0])
df = df.rename(columns={'Valor RD (2mm gap) (kV/2mm)': 'RD', 'Valor H2O (ppm)': 'H20'})

if 'FECHA DE MUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE MUESTRA': 'FECHA'})
elif 'FECHA DE\nMUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE\nMUESTRA': 'FECHA'})

df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
df = df.dropna(subset=['FECHA'])

# ---------------------------
# PUNTAJES Y CÁLCULO OLTC - MODIFICADO
# ---------------------------
# Solo asignar puntaje donde hay valores, dejar NaN donde no hay
df['Puntaje_RD'] = np.where(df['RD'].notna(), 
                           np.where(df['RD'] < 30, 5, 1), 
                           np.nan)

df['Puntaje_H2O'] = np.where(df['H20'].notna(), 
                            np.where(df['H20'] > 30, 5, 1), 
                            np.nan)

# Calcular OLTC solo si ambos puntajes tienen valores
df['OLTC'] = np.where(df['Puntaje_RD'].notna() & df['Puntaje_H2O'].notna(),
                     (5 * df['Puntaje_RD'] + 3 * df['Puntaje_H2O']) / 8,
                     np.nan)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_full = df[['SERIE', 'FECHA', 'OLTC', 'RD', 'H20']].copy()
df_OLTC = df[['SERIE', 'FECHA', 'OLTC']].copy()

# ---------------------------
# EXTENSIÓN DEL CALENDARIO
# ---------------------------
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- Tabla extendida de OLTC ----------
ultimos_2014 = df_OLTC[df_OLTC['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014['FECHA'] = fecha_inicio
base_ext = pd.concat([df_OLTC, ultimos_2014], ignore_index=True)

df_OLTC_ext = pd.merge(df_calendario, base_ext, on=["SERIE", "FECHA"], how="left")
df_OLTC_ext = df_OLTC_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla extendida de detalles ----------
ultimos_2014_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2014_det], ignore_index=True)

df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE", "FECHA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_OLTC():
    """Retorna la tabla OLTC con fechas originales."""
    return df_OLTC

def get_df_extendida_OLTC():
    """Retorna la tabla OLTC con fechas extendidas."""
    return df_OLTC_ext

def get_df_detalles_OLTC():
    """Retorna la tabla de detalles OLTC con fechas originales."""
    return df_full

def get_df_detalles_ext_OLTC():
    """Retorna la tabla de detalles OLTC con fechas extendidas."""
    return df_detalles_ext

# ---------------------------
# MOSTRAR RESULTADOS
# ---------------------------
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_OLTC().head(), '\n')

print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_OLTC().head(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_OLTC().head(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_OLTC().head(), '\n')
