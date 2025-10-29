# =====================================
# IMPORTAR LIBRERÍAS
# =====================================
import pandas as pd
import os
# =====================================
# CARGA DE DATOS
# =====================================
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/ACE.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/ACE.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/ACE.xlsx'
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

# =====================================
# LIMPIEZA Y FORMATEO DE DATOS
# =====================================
df.columns = df.columns.str.replace(r"\n", " ", regex=True).str.strip()

cols_drop = [
    "Unnamed: 0",
    "Temp. De muestra (°C)",
    "Examen Visual (ASTM 1524)",
    "Gravedad Específica (ASTM D1298, g/mL)",
    "Humedad ambiente (%)",
]
df = df.drop(columns=cols_drop)

if 'FECHA DE MUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE MUESTRA': 'FECHA'})
elif 'FECHA DE\nMUESTRA' in df.columns:
    df = df.rename(columns={'FECHA DE\nMUESTRA': 'FECHA'})
df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
df["TENSION"] = df["TENSION"].str.split("/").str[0]

rename_map = {
    "Contenido de humedad (ASTM D1533, ppm)": "HU",
    "Rigidez Dieléctrica (ASTM D1816, kV/2 mm)": "RD",
    "Tensión Interfacial (ASTM D971, mN/m)": "TIF",
    "Indice de neutralización (ASTM D974, mg KOH/g)": "AC",
    "Color (ASTM D1500)": "CO",
    "Factor de potencia a 25°C (ASTM D924, %)": "FP25",
    "Factor de potencia a 100°C (ASTM D924, %)": "FP100",
    "Contenido de inhibidor (ASTM D-2668)": "IO",
}
df = df.rename(columns=rename_map)

orden = ["SERIE", "TENSION", "FECHA", "FP25", "FP100", "HU", "AC", "TIF", "CO", "RD", "IO"]
df = df[orden]

num_cols = ["FP25", "FP100", "HU", "AC", "TIF", "CO", "RD", "IO"]
df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
df["SERIE"] = df["SERIE"].astype(str)
# Guardar tabla original sin tensión
# df_full = df.drop(columns=["TENSION"]).copy()
df_full = df.drop(columns=["TENSION"]).copy()
# =====================================
# PUNTAJES IEEE
# =====================================
score = [5, 3, 1]

reglas = {
    "220": {
        "FP25": [(lambda x: x > 0.3, score[0]), (lambda x: 0.1 < x <= 0.3, score[1]), (lambda x: x <= 0.1, score[2])],
        "FP100": [(lambda x: x > 4, score[0]), (lambda x: 3 < x <= 4, score[1]), (lambda x: x <= 3, score[2])],
        "HU": [(lambda x: x > 30, score[0]), (lambda x: 20 < x <= 30, score[1]), (lambda x: x <= 20, score[2])],
        "AC": [(lambda x: x > 0.10, score[0]), (lambda x: 0.05 < x <= 0.10, score[1]), (lambda x: x <= 0.05, score[2])],
        "TIF": [(lambda x: x < 28, score[0]), (lambda x: 28 <= x < 32, score[1]), (lambda x: x >= 32, score[2])],
        "CO": [(lambda x: x > 3.5, score[0]), (lambda x: x <= 3.5, score[2])],
        "RD": [(lambda x: x < 45, score[0]), (lambda x: 45 <= x < 50, score[1]), (lambda x: x >= 50, score[2])],
        "IO": [(lambda x: x < 0.1, score[0]), (lambda x: 0.1 <= x < 0.2, score[1]), (lambda x: x >= 0.2, score[2])],
    },
    "60": {
        "FP25": [(lambda x: x > 0.3, score[0]), (lambda x: 0.1 < x <= 0.3, score[1]), (lambda x: x <= 0.1, score[2])],
        "FP100": [(lambda x: x > 4, score[0]), (lambda x: 3 < x <= 4, score[1]), (lambda x: x <= 3, score[2])],
        "HU": [(lambda x: x > 40, score[0]), (lambda x: 30 < x <= 40, score[1]), (lambda x: x <= 30, score[2])],
        "AC": [(lambda x: x > 0.10, score[0]), (lambda x: 0.05 < x <= 0.10, score[1]), (lambda x: x <= 0.05, score[2])],
        "TIF": [(lambda x: x < 28, score[0]), (lambda x: 28 <= x < 32, score[1]), (lambda x: x >= 32, score[2])],
        "CO": [(lambda x: x > 3.5, score[0]), (lambda x: x <= 3.5, score[2])],
        "RD": [(lambda x: x < 35, score[0]), (lambda x: 35 <= x < 40, score[1]), (lambda x: x >= 40, score[2])],
        "IO": [(lambda x: x < 0.1, score[0]), (lambda x: 0.1 <= x < 0.2, score[1]), (lambda x: x >= 0.2, score[2])],
    },
}

def puntaje_parametro(valor, tension, parametro):
    if pd.isna(valor):
        return float("nan")
    reglas_tension = reglas.get(str(tension), {})
    condiciones = reglas_tension.get(parametro, [])
    for condicion, puntaje in condiciones:
        if condicion(valor):
            return puntaje
    return 1

for p in num_cols:
    df[f"Puntaje_{p}"] = df.apply(lambda row: puntaje_parametro(row[p], row["TENSION"], p), axis=1)
df_otro = df.copy()
# =====================================
# CÁLCULO ACE
# =====================================
pesos = {
    "Puntaje_FP25": 3,
    "Puntaje_FP100": 3,
    "Puntaje_HU": 4,
    "Puntaje_AC": 2,
    "Puntaje_TIF": 3,
    "Puntaje_CO": 1,
    "Puntaje_RD": 5,
    "Puntaje_IO": 1,
}

def calcular_ACE(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

df["ACE"] = df.apply(lambda row: calcular_ACE(row, pesos), axis=1)
df_ACE = df[["SERIE", "FECHA", "ACE"]]

# =====================================
# EXTENSIÓN DEL CALENDARIO (DESDE 2025)
# =====================================
inicio = "2015-01-01"
desde_2025 = f"{pd.Timestamp.today().year}-01-01"
fecha_inicio = pd.Timestamp(inicio)  # en el 2026 cambiar ---****
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE","FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# Última medición antes de 2025
ultimos_2024 = df_ACE[df_ACE['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_ACE, ultimos_2024], ignore_index=True)
df_extendida = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida = df_extendida.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# Extender detalles
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)
df_extendida_detalles = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_extendida_detalles = df_extendida_detalles.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# =====================================
# DETALLES + ACE
# =====================================
df_detalles = pd.merge(df_full, df_ACE, on=["SERIE","FECHA"], how="left")
df_detalles_ext = pd.merge(df_extendida_detalles, df_extendida, on=["SERIE","FECHA"], how="left")

# Reordenar para que ACE quede después de FECHA
def reordenar(df_in):
    cols = list(df_in.columns)
    if "ACE" in cols:
        cols.remove("ACE")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["ACE"] + cols[idx:]
    return df_in[cols]

df_detalles = reordenar(df_detalles)
df_detalles_ext = reordenar(df_detalles_ext)

# =====================================
# FUNCIONES
# =====================================
def get_df_ACE():
    return df_ACE

def get_df_extendida_ACE():
    return df_extendida

def get_df_detalles_ACE():
    return df_detalles

def get_df_detalles_ext_ACE():
    return df_detalles_ext

# =====================================
# PRUEBA
# =====================================
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_ACE())
print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_ACE().head())
print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_ACE().head())
print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_ACE().tail())

