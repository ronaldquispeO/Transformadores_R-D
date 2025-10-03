import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/DGA.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/DGA.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/DGA.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):   # verifica si existe
        df = pd.read_excel(path,header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")


# ---------------------------
# LIMPIEZA DE DATOS
# ---------------------------
df = df.drop(columns=['Unnamed: 0'])
# df["TENSION"] = df["TENSION"].str.split("/").str[0]
df = df.iloc[:, :10]

# Intentar renombrar la columna de fecha correctamente
print("Columnas originales:", list(df.columns))
if "FECHA DE\nMUESTRA" in df.columns:
    df = df.rename(columns={"FECHA DE\nMUESTRA": "FECHA"})
elif "FECHA DE MUESTRA" in df.columns:
    df = df.rename(columns={"FECHA DE MUESTRA": "FECHA"})
elif "FECHA" not in df.columns:
    raise KeyError("No se encontró ninguna columna de fecha reconocida en el archivo. Columnas: " + str(list(df.columns)))

df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
df = df.dropna(subset=['FECHA'])

# Guardar tabla original (solo fechas de medición)
# df_full = df.drop(columns=["TENSION"]).copy()
df_full = df.copy()
# ---------------------------
# LÍMITES Y GASES
# ---------------------------
limite = {
    'H2': {'concentracion': 150, 'tasa': 132},
    'CH4': {'concentracion': 130, 'tasa': 120},
    "C2H2": {"concentracion": 20, "tasa": 4},
    "C2H4": {"concentracion": 280, "tasa": 146},
    "C2H6": {"concentracion": 90, "tasa": 90},
    "CO": {"concentracion": 600, "tasa": 1060},
    "CO2": {"concentracion": 14000, "tasa": 10000}
}
gases2 = list(limite.keys())

# ---------------------------
# CONCENTRACIÓN
# ---------------------------
for gas in gases2:
    df[f'concentracion_{gas}'] = df.apply(lambda g: int(g[gas] > limite[gas]['concentracion']), axis=1)

# ---------------------------
# TASA DE INCREMENTO ANUAL
# ---------------------------
for gas in gases2:
    df[f"tasa_{gas}"] = df.groupby("SERIE").apply(
    lambda g: (g[gas].diff(-1) / ((g["FECHA"] - g["FECHA"].shift(-1)).dt.days) * 365)
    ).reset_index(level=0, drop=True)

# ---------------------------
# FLAGS Y PUNTAJE
# ---------------------------
for gas, valores in limite.items():
    df[f"flag_{gas}"] = (df[f"tasa_{gas}"] > valores['tasa']).astype(int)

def puntaje_gas(row, gas):
    conc = row[f'concentracion_{gas}']
    tasa = row[f'flag_{gas}']
    if conc > 0 and tasa > 0:
        return 5
    elif conc > 0 and tasa <= 0:
        return 4
    elif conc <= 0 and tasa > 0:
        return 3
    else:
        return 1

for gas in gases2:
    df[f'puntaje_{gas}'] = df.apply(lambda row: puntaje_gas(row, gas), axis=1)

# ---------------------------
# CÁLCULO DGA
# ---------------------------
weight = {"C2H2":5,"H2":2,"CH4":3,"C2H4":4,"C2H6":3,"CO":2,"CO2":1}
total_weight = sum(weight.values())
df['DGA'] = df.apply(lambda row: sum(row[f'puntaje_{gas}']*weight[gas] for gas in gases2)/total_weight, axis=1)

df_DGA = df[['SERIE','FECHA','DGA']]

# ---------------------------
# EXTENSIÓN DEL CALENDARIO DESDE 2025
# ---------------------------
inicio = "2015-01-01"
desde_2025 = f"{pd.Timestamp.today().year}-01-01"
fecha_inicio = pd.Timestamp(inicio)  # en el 2026 cambiar ---****
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")

# Calendario
todas_series = df['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE","FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# ---------- Tabla extendida de DGA ----------
ultimos_2024 = df_DGA[df_DGA['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024['FECHA'] = fecha_inicio
base_ext = pd.concat([df_DGA, ultimos_2024], ignore_index=True)

df_extendida = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_extendida = df_extendida.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------- Tabla extendida de detalles ----------
ultimos_2024_det = df_full[df_full['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2024_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_full, ultimos_2024_det], ignore_index=True)

df_extendida_detalles = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_extendida_detalles = df_extendida_detalles.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# ---------------------------
# DETALLES + DGA
# ---------------------------
df_detalles = pd.merge(df_full, df_DGA, on=["SERIE","FECHA"], how="left")
df_detalles_ext = pd.merge(df_extendida_detalles, df_extendida, on=["SERIE","FECHA"], how="left")

# Reordenar columnas: poner DGA después de FECHA
def reordenar(df_in):
    cols = list(df_in.columns)
    if "DGA" in cols:
        cols.remove("DGA")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["DGA"] + cols[idx:]
    return df_in[cols]

df_detalles = reordenar(df_detalles)
df_detalles_ext = reordenar(df_detalles_ext)

# ---------------------------
# FUNCIONES PARA LLAMAR
# ---------------------------
def get_df_DGA():
    return df_DGA

def get_df_extendida_DGA():
    return df_extendida

def get_df_detalles_DGA():
    return df_detalles

def get_df_detalles_ext_DGA():
    return df_detalles_ext

print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_DGA(), '\n')

print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_DGA().head(), '\n')

print('\n ====== TABLA DE DETALLES DE DGA CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_DGA().head(), '\n')

print('\n ====== TABLA DE DETALLES DE DGA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_DGA().head(), '\n')
