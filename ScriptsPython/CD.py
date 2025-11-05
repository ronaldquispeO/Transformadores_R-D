import pandas as pd
import numpy as np
import os

# ---------------------------
# CARGA DE ARCHIVO
# ---------------------------
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPDEVANADO.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPDEVANADO.xlsx'
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
# df["SERIE"] = df["SERIE"].astype(str)
df.columns = df.columns.str.replace(r"\n", " ", regex=True).str.strip()
df = df.dropna(axis=1, how="all")

# Seleccionar columnas que terminan con 'pF'
cols_cd = ["SERIE", "FECHA"] + [c for c in df.columns if c.endswith("pF")]
df_cd = df[cols_cd].copy()

# Aseguramos que FECHA sea datetime
df_cd["FECHA"] = pd.to_datetime(df_cd["FECHA"], errors="coerce")

# ---------------------------
# CÁLCULO DE DELTAS
# ---------------------------
cap_cols = [c for c in df_cd.columns if c.endswith("pF")]

for col in cap_cols:
    ref = df_cd.groupby("SERIE").apply(lambda g: g.loc[g["FECHA"].idxmin(), col])
    ref_mapped = df_cd["SERIE"].map(ref)

    delta = abs((df_cd[col] - ref_mapped) / ref_mapped) * 100
    delta = delta.replace([np.inf, -np.inf], np.nan)

    mask = df_cd[col].isna() | ref_mapped.isna() | (ref_mapped == 0) | (df_cd[col] == 0)
    delta[mask] = np.nan

    df_cd[f"{col}_Delta"] = delta

df_cd['Max_CD'] = df_cd[[c for c in df_cd.columns if c.endswith("Delta")]].max(axis=1)

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE
# ---------------------------
df_cd['CD'] = np.select(
    [
        df_cd['Max_CD'] > 10,
        (df_cd['Max_CD'] > 5) & (df_cd['Max_CD'] <= 10),
        df_cd['Max_CD'] <= 5
    ],
    [5, 3, 1],
    default=np.nan
)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_full = df_cd.drop(columns='Max_CD')               # detallada (SERIE, FECHA, pF, Delta, CD)
df_CD = df_cd[['SERIE', 'FECHA', 'CD']].copy()       # original (SERIE, FECHA, CD)

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

# ---------- Tabla extendida CD ----------
ultimos_2024 = df_CD[df_CD['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_CD, ultimos_2024], ignore_index=True)

df_extendida_CD = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_CD = df_extendida_CD.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla detallada extendida ----------
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)

df_detalles_ext_CD = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext_CD = df_detalles_ext_CD.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_CD():
    return df_CD

def get_df_extendida_CD():
    return df_extendida_CD

def get_df_detalles_CD():
    # Quitar columnas que terminan en Delta
    cols = [c for c in df_full.columns if not c.endswith('Delta')]
    # Reordenar: SERIE, FECHA, CD, ...resto
    base = ['SERIE', 'FECHA', 'CD']
    resto = [c for c in cols if c not in base]
    orden = base + resto
    return df_full[orden]

def get_df_detalles_ext_CD():
    # Quitar columnas que terminan en Delta
    cols = [c for c in df_detalles_ext_CD.columns if not c.endswith('Delta')]
    # Reordenar: SERIE, FECHA, CD, ...resto
    base = ['SERIE', 'FECHA', 'CD']
    resto = [c for c in cols if c not in base]
    orden = base + resto
    return df_detalles_ext_CD[orden]

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA CD ORIGINAL ====== \n")
print(get_df_CD(), "\n")

print("\n ====== TABLA CD EXTENDIDA ====== \n")
print(get_df_extendida_CD().head(), "\n")

print("\n ====== TABLA DETALLADA CD ====== \n")
print(get_df_detalles_CD().head(), "\n")

print("\n ====== TABLA DETALLADA EXTENDIDA CD ====== \n")
print(get_df_detalles_ext_CD().tail(), "\n")