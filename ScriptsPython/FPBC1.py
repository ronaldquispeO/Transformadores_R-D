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
df = df.drop(columns=df.columns[0])  # elimina la primera columna vacía
cols_base = ['SERIE', 'FECHA']
cols_fp = [col for col in df.columns if col.endswith('%')]

df_fp = df[cols_base + cols_fp]  # dataset base solo con %s

# ---------------------------
# SUBTABLA C1
# ---------------------------
cols_fp_c1 = [col for col in df_fp.columns if col.startswith('C1')]
df_fp_c1 = df_fp[cols_base + cols_fp_c1].copy()

# máximo de FP C1 (solo auxiliar, no se mostrará en tablas finales)
df_fp_c1['Max_FP_C1'] = df_fp_c1[cols_fp_c1].max(axis=1)

# ---------------------------
# CONFIGURACIÓN DE PUNTAJE
# ---------------------------
pesos = [5, 3, 1]  
limites = {"FP": [1.0, 0.5]}  # [límite superior, límite inferior]

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

# aplicar regla IEEE C57.152-2013
df_fp_c1 = asignar_puntaje(df_fp_c1, 'Max_FP_C1', 'FPBC1', limites["FP"], pesos)

# ---------------------------
# TABLAS BASE
# ---------------------------
df_FP_C1 = df_fp_c1[['SERIE', 'FECHA', 'FPBC1']].copy()   # tabla base

# tabla detallada → quitamos Max_FP_C1 y movemos FPBC1 a la tercera columna
df_full_C1 = df_fp_c1[['SERIE', 'FECHA', 'FPBC1'] + cols_fp_c1].copy()

# ---------------------------
# EXTENSIÓN DEL CALENDARIO DESDE 2015
# ---------------------------
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- tabla extendida FPBC1 ----------
ultimos_2015 = df_FP_C1[df_FP_C1['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2015['FECHA'] = fecha_inicio
base_ext = pd.concat([df_FP_C1, ultimos_2015], ignore_index=True)

df_extendida_FP_C1 = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida_FP_C1 = df_extendida_FP_C1.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- tabla detallada extendida ----------
ultimos_2015_det = df_full_C1[df_full_C1['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2015_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full_C1, ultimos_2015_det], ignore_index=True)

df_detalles_ext_FP_C1 = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext_FP_C1 = df_detalles_ext_FP_C1.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_FP_C1():
    return df_FP_C1

def get_df_extendida_FP_C1():
    return df_extendida_FP_C1

def get_df_detalles_FP_C1():
    return df_full_C1

def get_df_detalles_ext_FP_C1():
    return df_detalles_ext_FP_C1

# ---------------------------
# DEMO DE RESULTADOS
# ---------------------------
print("\n ====== TABLA FPBC1 ORIGINAL ====== \n")
print(get_df_FP_C1().tail(), "\n")

print("\n ====== TABLA FPBC1 EXTENDIDA ====== \n")
print(get_df_extendida_FP_C1().tail(), "\n")

print("\n ====== TABLA DETALLADA FPBC1 ====== \n")
print(get_df_detalles_FP_C1().head(), "\n")

print("\n ====== TABLA DETALLADA EXTENDIDA FPBC1 ====== \n")
print(get_df_detalles_ext_FP_C1().tail(), "\n")

