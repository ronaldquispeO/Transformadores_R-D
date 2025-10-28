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

# ---------------------------
# LIMPIEZA DE DATOS
# ---------------------------
df["SERIE"] = df["SERIE"].astype(str)
df.columns = df.columns.str.replace(r"\n", " ", regex=True).str.strip()
df = df.dropna(axis=1, how="all")

# Seleccionar columnas que terminan en %
cols_fp = ["SERIE", "FECHA"] + [c for c in df.columns if c.endswith("%")]
df_fp = df[cols_fp]

# Columna con el máximo valor de FPDEVANADO
df_fp['Max_FP'] = df_fp[[c for c in df_fp.columns if c.endswith('%')]].max(axis=1)

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE
# ---------------------------
pesos = [5, 3, 1]
limites = {"FPDEVANADO": [1.0, 0.5]}

def asignar_puntaje(df, columna_valor, columna_puntaje, limites, pesos):
    df[columna_puntaje] = np.select(
        [
            df[columna_valor] > limites[0],
            (df[columna_valor] > limites[1]) & (df[columna_valor] <= limites[0]),
            df[columna_valor] <= limites[1]
        ],
        pesos,
        default=np.nan
    )
    return df

df_fp = asignar_puntaje(df_fp, 'Max_FP', 'FPDEVANADO', limites["FPDEVANADO"], pesos)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_full = df_fp.drop(columns='Max_FP')              # detallada (SERIE, FECHA, %, FPDEVANADO)
df_FP = df_fp[['SERIE', 'FECHA', 'FPDEVANADO']].copy()      # original (SERIE, FECHA, FPDEVANADO)

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

# ---------- Tabla extendida FPDEVANADO ----------
ultimos_2024 = df_FP[df_FP['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_FP, ultimos_2024], ignore_index=True)

df_extendida_FP = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_FP = df_extendida_FP.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla detallada extendida ----------
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)

df_detalles_ext_FP = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext_FP = df_detalles_ext_FP.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_FP():
    return df_FP

def get_df_extendida_FP():
    return df_extendida_FP

def get_df_detalles_FP():
    return df_full

def get_df_detalles_ext_FP():
    return df_detalles_ext_FP

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA FPDEVANADO ORIGINAL ====== \n")
print(get_df_FP(), "\n")

print("\n ====== TABLA FPDEVANADO EXTENDIDA ====== \n")
print(get_df_extendida_FP().head(), "\n")

print("\n ====== TABLA DETALLADA FPDEVANADO ====== \n")
print(get_df_detalles_FP().head(), "\n")

print("\n ====== TABLA DETALLADA EXTENDIDA FPDEVANADO ====== \n")
print(get_df_detalles_ext_FP().tail(), "\n")


