import numpy as np
import pandas as pd
import os

# ---------------------------
# CARGA DE ARCHIVO
# ---------------------------
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPC1C2.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPC1C2.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/FPC1C2.xlsx'
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
df = df.drop(columns=df.columns[0])
cols_base = ['SERIE', 'FECHA']

# quedarnos solo con columnas de capacitancia (pF)
cols_c = cols_base + [col for col in df.columns if col.endswith('pF')]
df_c = df[cols_c].copy()

# ---------------------------
# SUBTABLA C2
# ---------------------------
cols_c_c2 = [col for col in df_c.columns if col.startswith('C2')]
df_c_c2 = df_c[cols_base + cols_c_c2].copy()

# FECHA a datetime
df_c_c2["FECHA"] = pd.to_datetime(df_c_c2["FECHA"])

# calcular % de variación respecto al valor inicial por SERIE
cap_cols = [c for c in df_c_c2.columns if c.endswith("pF")]
for col in cap_cols:
    ref = df_c_c2.groupby("SERIE").apply(lambda g: g.loc[g["FECHA"].idxmin(), col])
    ref_mapped = df_c_c2["SERIE"].map(ref)
    delta = abs((df_c_c2[col] - ref_mapped) / ref_mapped) * 100
    delta = delta.replace([np.inf, -np.inf], np.nan)
    mask = df_c_c2[col].isna() | ref_mapped.isna() | (ref_mapped == 0) | (df_c_c2[col] == 0)
    delta[mask] = np.nan
    df_c_c2[f"{col}_Delta"] = delta

# máximo de variaciones
cols_c_c2_delta = [col for col in df_c_c2.columns if col.endswith('Delta')]
df_c_c2["Max_C_C2"] = df_c_c2[cols_c_c2_delta].max(axis=1)

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE
# ---------------------------
pesos = [5,1]  
limites = {"C": [1]}  # [límite superior, límite inferior]

def asignar_puntaje(df, columna_valor, columna_puntaje, limites, pesos):
    df[columna_puntaje] = np.select(
        [
            df[columna_valor] > limites[0],
            df[columna_valor] <= limites[0]
        ],
        pesos,
        default=np.nan
    )
    return df

# aplicar reglas
df_c_c2 = asignar_puntaje(df_c_c2, "Max_C_C2", "CBC2", limites["C"], pesos)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_C_C2 = df_c_c2[["SERIE", "FECHA", "CBC2"]].copy()  # tabla original
df_full_C2 = df_c_c2[["SERIE", "FECHA", "CBC2"] + cap_cols].copy()  # tabla detallada

# ---------------------------
# EXTENSIÓN DEL CALENDARIO DESDE 2015
# ---------------------------
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

todas_series = df["SERIE"].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- tabla extendida CBC2 ----------
ultimos_2015 = df_C_C2[df_C_C2["FECHA"] < fecha_inicio].sort_values("FECHA").groupby("SERIE").tail(1)
ultimos_2015["FECHA"] = fecha_inicio
base_ext = pd.concat([df_C_C2, ultimos_2015], ignore_index=True)

df_extendida_C_C2 = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_C_C2 = df_extendida_C_C2.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- tabla detallada extendida ----------
ultimos_2015_det = df_full_C2[df_full_C2["FECHA"] < fecha_inicio].sort_values("FECHA").groupby("SERIE").tail(1)
ultimos_2015_det["FECHA"] = fecha_inicio
base_ext_det = pd.concat([df_full_C2, ultimos_2015_det], ignore_index=True)

df_detalles_ext_C_C2 = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext_C_C2 = df_detalles_ext_C_C2.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_C_C2():
    return df_C_C2

def get_df_extendida_C_C2():
    return df_extendida_C_C2

def get_df_detalles_C_C2():
    return df_full_C2

def get_df_detalles_ext_C_C2():
    return df_detalles_ext_C_C2

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA CBC2 ORIGINAL ====== \n")
print(get_df_C_C2(), "\n")

print("\n ====== TABLA CBC2 EXTENDIDA ====== \n")
print(get_df_extendida_C_C2().head(), "\n")

print("\n ====== TABLA DETALLADA CBC2 ====== \n")
print(get_df_detalles_C_C2().head(), "\n")

print("\n ====== TABLA DETALLADA EXTENDIDA CBC2 ====== \n")
print(get_df_detalles_ext_C_C2().head(), "\n")
