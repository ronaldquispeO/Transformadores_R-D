import pandas as pd
import numpy as np
from ARRrohm import get_df_extendida_ROHM, get_df_ROHM
from ARRrtra import get_df_extendida_RTRA, get_df_RTRA
from ARRdis import get_df_DIS, get_df_extendida_DIS
from functools import reduce

# =============================
# PESOS DE LOS ÍNDICES
# =============================
pesos_ARR = {
    'ROHM': 3,
    'RTRA': 5,
    'ZCC': 5,
}
# =============================
# FUNCIÓN PARA CALCULAR ARR
# =============================
def calcular_ARR(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

# =============================
# FUNCIÓN PRINCIPAL
# =============================
def get_df_detalles_ARR():
    ROHM = get_df_ROHM()
    RTRA = get_df_RTRA()
    RDIS = get_df_DIS()
    tablas = {
        "ROHM": ROHM,
        "RTRA": RTRA,
        "RDIS": RDIS,
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

    # Ordenamos por SERIE y FECHA
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # ============================
    # Calcular ARR
    # ============================
    resultado["ARR"] = resultado.apply(lambda row: calcular_ARR(row, pesos_ARR), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "ARR"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

def get_df_detalles_ext_ARR():
    ROHM = get_df_extendida_ROHM()
    RTRA = get_df_extendida_RTRA()
    RDIS = get_df_extendida_DIS()
    tablas = {
        "ROHM": ROHM,
        "RTRA": RTRA,
        "RDIS": RDIS
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

    # Ordenamos por SERIE y FECHA
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # ============================
    # Calcular ARR
    # ============================
    resultado["ARR"] = resultado.apply(lambda row: calcular_ARR(row, pesos_ARR), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "ARR"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado




df_ARR_detalles = get_df_detalles_ext_ARR()
df_ARR = df_ARR_detalles[['SERIE','FECHA','ARR']]
df_ARR = df_ARR.rename(columns={'FECHA DE MUESTRA': 'FECHA'})

def get_df_extendida_ARR():
    return df_ARR

print(get_df_detalles_ARR().head())
print(get_df_detalles_ext_ARR())

