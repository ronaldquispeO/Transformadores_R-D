import numpy as np
import pandas as pd
import os

# ---------------------------
# CARGA DE ARCHIVO
# ---------------------------
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPCOCA.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/FPCOCA.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/FPCOCA.xlsx'
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

# calcular máximo de corriente
df['Max_CC'] = df.iloc[:, 3:].max(axis=1)

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE
# ---------------------------
pesos = [5,1]  
limites = {"CC": [100]}  # [límite superior]

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
df = asignar_puntaje(df, "Max_CC", "CC", limites["CC"], pesos)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_CC = df[["SERIE", "FECHA", "CC"]].copy()   # tabla original
df_full_CC = df[["SERIE", "FECHA", "CC"] + [c for c in df.columns if c not in ["SERIE","FECHA","CC","Max_CC"]]].copy()  # tabla detallada

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

# ---------- tabla extendida CC ----------
ultimos_2015 = df_CC[df_CC["FECHA"] < fecha_inicio].sort_values("FECHA").groupby("SERIE").tail(1)
ultimos_2015["FECHA"] = fecha_inicio
base_ext = pd.concat([df_CC, ultimos_2015], ignore_index=True)

df_extendida_CC = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_CC = df_extendida_CC.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- tabla detallada extendida ----------
ultimos_2015_det = df_full_CC[df_full_CC["FECHA"] < fecha_inicio].sort_values("FECHA").groupby("SERIE").tail(1)
ultimos_2015_det["FECHA"] = fecha_inicio
base_ext_det = pd.concat([df_full_CC, ultimos_2015_det], ignore_index=True)

df_detalles_ext_CC = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext_CC = df_detalles_ext_CC.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_CC():
    return df_CC

def get_df_extendida_CC():
    return df_extendida_CC

def get_df_detalles_CC():
    return df_full_CC

def get_df_detalles_ext_CC():
    return df_detalles_ext_CC

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA CC ORIGINAL ====== \n")
print(get_df_CC(), "\n")

print("\n ====== TABLA CC EXTENDIDA ====== \n")
print(get_df_extendida_CC().head(), "\n")

print("\n ====== TABLA DETALLADA CC ====== \n")
print(get_df_detalles_CC().head(), "\n")

print("\n ====== TABLA DETALLADA EXTENDIDA CC ====== \n")
print(get_df_detalles_ext_CC().head(), "\n")
