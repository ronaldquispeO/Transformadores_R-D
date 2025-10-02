import pandas as pd
import numpy as np
import os

# ---------------------------
# CARGA DE ARCHIVO
# ---------------------------
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FURANOS.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FURANOS.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):
        df = pd.read_excel(path, header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")

# ---------------------------
# LIMPIEZA DE DATOS
# ---------------------------
df = df.dropna(axis=1, how='all')
df['Fecha'] = pd.to_datetime(df['Fecha'], errors="coerce")

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE FUR
# ---------------------------
df['FUR'] = np.select(
    [
        df['2-Furfuraldehido (FAL, ppb)'] > 5000,
        (df['2-Furfuraldehido (FAL, ppb)'] > 1000) & (df['2-Furfuraldehido (FAL, ppb)'] <= 5000),
        (df['2-Furfuraldehido (FAL, ppb)'] > 500) & (df['2-Furfuraldehido (FAL, ppb)'] <= 1000),
        (df['2-Furfuraldehido (FAL, ppb)'] > 100) & (df['2-Furfuraldehido (FAL, ppb)'] <= 500),
        df['2-Furfuraldehido (FAL, ppb)'] <= 100
    ],
    [5, 4, 3, 2, 1],
    default=np.nan
)

# ---------------------------
# TABLAS BASE
# ---------------------------
df = df.rename(columns={'Fecha': 'FECHA'})
df_FUR = df[['SERIE', 'FECHA', 'FUR']].copy()

# ---------------------------
# EXTENSIÓN DEL CALENDARIO DESDE 2015
# ---------------------------
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)   # en el 2026 cambiar si hace falta
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- Tabla extendida FUR ----------
ultimos_2024 = df_FUR[df_FUR['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_FUR, ultimos_2024], ignore_index=True)

df_extendida_FUR = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_FUR = df_extendida_FUR.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla de detalles originales (datos + FUR) ----------
df_detalles_FUR = df.copy()   # ya contiene FUR

# ---------- Tabla de detalles extendida ----------
ultimos_2024_det = df[df['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df, ultimos_2024_det], ignore_index=True)

df_extendida_detalles_FUR = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_extendida_detalles_FUR = df_extendida_detalles_FUR.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# esta ya tiene FUR, no necesitas mergear otra vez
df_detalles_ext_FUR = df_extendida_detalles_FUR.copy()

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_FUR():
    return df_FUR

def get_df_extendida_FUR():
    return df_extendida_FUR

def get_df_detalles_FUR():
    return df_detalles_FUR

def get_df_detalles_ext_FUR():
    return df_detalles_ext_FUR

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA CON FECHAS ORIGINALES ====== \n")
print(get_df_FUR(), "\n")

print("\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n")
print(get_df_extendida_FUR(), "\n")

print("\n ====== TABLA DE DETALLES DE FUR CON FECHAS ORIGINALES ====== \n")
print(get_df_detalles_FUR(), "\n")

print("\n ====== TABLA DE DETALLES DE FUR CON FECHAS EXTENDIDAS ====== \n")
print(get_df_detalles_ext_FUR(), "\n")
