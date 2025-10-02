import pandas as pd
import numpy as np
from FP import get_df_FP, get_df_extendida_FP
from CD import get_df_CD, get_df_extendida_CD
from FURANOS import get_df_FUR, get_df_extendida_FUR
from functools import reduce

# =============================
# PESOS DE LOS ÍNDICES
# =============================
pesos_AIS = {
    'V': 1,
    'ICC': 5,
    'FUR': 2,
    'FPDEVANADO':2,
    'CD':3
}
# =============================
# FUNCIÓN PARA CALCULAR AIS
# =============================
def calcular_AIS(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

# =============================
# FUNCIÓN PRINCIPAL
# =============================
def get_df_detalles_AIS():
    FPDEVANADO = get_df_FP()
    CD = get_df_CD()
    FUR = get_df_FUR()
    tablas = {
        "FPDEVANADO": FPDEVANADO,
        "CD": CD,
        "FUR": FUR,
    }
    # Filtramos los que no sean None
    tablas = {k: v for k, v in tablas.items() if v is not None}

    if not tablas:
        return pd.DataFrame()
    
    # Merge progresivo de todas las tablas (asumimos que todas ya tienen SERIE + FECHA extendidos)
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    # Asegurar tipo datetime en FECHA
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")

    # Ordenamos por SERIE y FECHA DE MUESTRA
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # ============================
    # Calcular AIS
    # ============================
    resultado["AIS"] = resultado.apply(lambda row: calcular_AIS(row, pesos_AIS), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "AIS"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

def get_df_detalles_ext_AIS():
    FPDEVANADO = get_df_extendida_FP()
    CD = get_df_extendida_CD()
    FUR = get_df_extendida_FUR()
    tablas = {
        "FPDEVANADO": FPDEVANADO,
        "CD": CD,
        "FUR": FUR,
    }
    # Filtramos los que no sean None
    tablas = {k: v for k, v in tablas.items() if v is not None}

    if not tablas:
        return pd.DataFrame()
    
    # Merge progresivo de todas las tablas (asumimos que todas ya tienen SERIE + FECHA extendidos)
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    # Asegurar tipo datetime en FECHA
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")

    # Ordenamos por SERIE y FECHA DE MUESTRA
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # ============================
    # Calcular AIS
    # ============================
    resultado["AIS"] = resultado.apply(lambda row: calcular_AIS(row, pesos_AIS), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "AIS"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

df_AIS_detalles = get_df_detalles_ext_AIS()
df_AIS = df_AIS_detalles[['SERIE','FECHA','AIS']]
df_AIS = df_AIS.rename(columns={'FECHA': 'FECHA DE MUESTRA'})

def get_df_extendida_AIS():
    return df_AIS

df_arr = get_df_detalles_AIS()
df_arr_detalles = df_arr[['SERIE','FECHA','AIS']]
print(df_arr.head())
print(get_df_detalles_ext_AIS().head())
