import numpy as np
import pandas as pd
import os
# ---------------------------
# LECTURA DE DATOS
# ---------------------------

# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RDEV.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/RDEV.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/RDEV.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):   # verifica si existe
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
df = df.iloc[:, 1:]   # quitar primera columna vacía
df['FECHA'] = pd.to_datetime(df['FECHA'], errors="coerce")
df = df.dropna(subset=['FECHA'])
# df["SERIE"] = df["SERIE"].astype(str)
df_full = df.copy()   # copia de detalles originales

# ---------------------------
# CÁLCULO DE DELTAS
# ---------------------------
res_cols = [c for c in df.columns if c.endswith("[mΩ]")]

for col in res_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')
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

# ---------------------------
# CÁLCULO DE SCORES
# ---------------------------
delta_cols = [c for c in df.columns if c.endswith("_Delta")]

for col in delta_cols:
    puntaje_col = col.replace("_Delta", "_Score")
    df[puntaje_col] = df[col].apply(
        lambda x: np.nan if pd.isna(x) else (1 if x < 5 else 5)
    )

# ---------------------------
# ROHM FINAL
# ---------------------------
score_cols = [c for c in df.columns if c.endswith("_Score")]
df["ROHM"] = df[score_cols].max(axis=1)
df_ROHM = df[['SERIE', 'FECHA', 'ROHM']]

# ---------------------------
# EXTENSIÓN DEL CALENDARIO DESDE 2025
# ---------------------------
inicio = "2015-01-01"
desde_2025 = f"{pd.Timestamp.today().year}-01-01"
fecha_inicio = pd.Timestamp(inicio)  # en el 2026 cambiar ---****
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- Tabla extendida ROHM ----------
ultimos_2024 = df_ROHM[df_ROHM['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_ROHM, ultimos_2024], ignore_index=True)
df_extendida = pd.merge(df_calendario, base_ext, on=["SERIE", "FECHA"], how="left")
df_extendida = df_extendida.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla extendida detalles ----------
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)
df_extendida_detalles = pd.merge(df_calendario, base_ext_det, on=["SERIE", "FECHA"], how="left")
df_extendida_detalles = df_extendida_detalles.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# DETALLES + ROHM
# ---------------------------
df_detalles = pd.merge(df_full, df_ROHM, on=["SERIE", "FECHA"], how="left")
df_detalles_ext = pd.merge(df_extendida_detalles, df_extendida, on=["SERIE", "FECHA"], how="left")

# Reordenar columnas: poner ROHM después de FECHA
def reordenar(df_in):
    cols = list(df_in.columns)
    if "ROHM" in cols:
        cols.remove("ROHM")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["ROHM"] + cols[idx:]
    return df_in[cols]

df_detalles = reordenar(df_detalles)
df_detalles_ext = reordenar(df_detalles_ext)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_ROHM():
    return df_ROHM

def get_df_extendida_ROHM():
    return df_extendida

def get_df_detalles_ROHM():
    return df_detalles

def get_df_detalles_ext_ROHM():
    return df_detalles_ext

# ---------------------------
# PRINT DE TABLAS
# ---------------------------
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_ROHM(), '\n')
print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_ROHM().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_ROHM().head(), '\n')
print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_ROHM().tail(), '\n')
