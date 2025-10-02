import numpy as np
import pandas as pd
import os
# Lista de posibles rutas
addresses = [
    'C:/Users/RONALD Q/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx',
    'C:/Users/roquispec/OneDrive - LUZ DEL SUR S.A.A/Documentos/Estudios de Ingreso/ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx',
    'C:/Users/mticllacu/OneDrive - LUZ DEL SUR S.A.A/Archivos de Ronald Quispe Ocaña - ProyectoRyD_V2/Basededatos/IEXCITACION.xlsx'
]

df = None
for path in addresses:
    if os.path.exists(path):   # verifica si existe
        df = pd.read_excel(path,header=1)
        print(f"✅ Archivo cargado desde: {path}")
        break

if df is None:
    raise FileNotFoundError("❌ No se encontró el archivo en ninguna de las rutas especificadas.")

df = df.drop(columns=['Unnamed: 0'])
df = df.rename(columns ={'FECHA DE MUESTRA':'FECHA'})


# Calcular columnas delta respecto al valor inicial por SERIE en la fecha mínima
# Detectar columnas numéricas a procesar (excluyendo SERIE y FECHA)
cols_val = [c for c in df.columns if c not in ['SERIE', 'FECHA'] and np.issubdtype(df[c].dtype, np.number)]

for col in cols_val:
    ref = df.groupby('SERIE').apply(lambda g: g.loc[g['FECHA'].idxmin(), col])
    ref_mapped = df['SERIE'].map(ref)
    delta = abs((df[col] - ref_mapped) / ref_mapped) * 100
    delta = delta.replace([np.inf, -np.inf], np.nan)
    mask = df[col].isna() | ref_mapped.isna() | (ref_mapped == 0) | (df[col] == 0)
    delta[mask] = np.nan
    df[f'{col}_Delta'] = delta
# Calcular el máximo de las columnas que terminan en 'Delta' para cada fila
cols_delta = [col for col in df.columns if col.endswith('Delta')]
df['Max_Iex'] = df[cols_delta].max(axis=1)

# Asignar puntaje IEX según la tabla SD MYERS
# Si Max_Iex > 10% → 5, si Max_Iex ≤ 10% → 1
df['IEX'] = np.where(df['Max_Iex'] > 10, 5, 1)

# Crear tabla final solo con SERIE, FECHA, IEX y las columnas que no terminan en 'Delta' ni son 'Max_Iex'
cols_final = [col for col in df.columns if not col.endswith('Delta') and col != 'Max_Iex']
# Aseguramos el orden: SERIE, FECHA, IEX primero
cols_final = ['SERIE', 'FECHA', 'IEX'] + [col for col in cols_final if col not in ['SERIE', 'FECHA', 'IEX']]
df_final = df[cols_final]
print(df_final)