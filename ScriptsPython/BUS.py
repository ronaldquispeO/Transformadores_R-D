import pandas as pd
import numpy as np
from FPBC1 import get_df_FP_C1, get_df_extendida_FP_C1
from FPBC2 import get_df_FP_C2, get_df_extendida_FP_C2
from CBC1 import get_df_C_C1, get_df_extendida_C_C1
from CBC2 import get_df_C_C2, get_df_extendida_C_C2
from FPCOCA import get_df_CC, get_df_extendida_CC
from functools import reduce

# =============================
# PESOS DE LOS ÍNDICES
# =============================
pesos_BUS = {
    'FPBC1': 4,
    'FPBC2': 2,
    'CBC1': 5,
    'CBC2': 3,
    'CC': 3
}
# =============================
# FUNCIÓN PARA CALCULAR BUS
# =============================
def calcular_BUS(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

# =============================
# FUNCIÓN PRINCIPAL
# =============================
def get_df_detalles_BUS():
    FPBC1 = get_df_FP_C1()
    FPBC2 = get_df_FP_C2()
    CBC1 = get_df_C_C1()
    CBC2 = get_df_C_C2()
    CC = get_df_CC()
    tablas = {
        "FPBC1": FPBC1,
        "FPBC2": FPBC2,
        "CBC1": CBC1,
        "CBC2": CBC2,
        "CC": CC,
    }
    tablas = {k: v for k, v in tablas.items() if v is not None}
    if not tablas:
        return pd.DataFrame()
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    # # Convertir FECHA a solo date (sin hora)
    # resultado["FECHA"] = resultado["FECHA"].dt.date
    resultado["BUS"] = resultado.apply(lambda row: calcular_BUS(row, pesos_BUS), axis=1)
    columnas_finales = ["SERIE", "FECHA", "BUS"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

def get_df_detalles_ext_BUS():
    FPBC1 = get_df_extendida_FP_C1()
    FPBC2 = get_df_extendida_FP_C2()
    CBC1 = get_df_extendida_C_C1()
    CBC2 = get_df_extendida_C_C2()
    CC = get_df_extendida_CC()
    tablas = {
        "FPBC1": FPBC1,
        "FPBC2": FPBC2,
        "CBC1": CBC1,
        "CBC2": CBC2,
        "CC": CC,
    }
    tablas = {k: v for k, v in tablas.items() if v is not None}
    if not tablas:
        return pd.DataFrame()
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    # # Convertir FECHA a solo date (sin hora)
    # resultado["FECHA"] = resultado["FECHA"].dt.date
    resultado["BUS"] = resultado.apply(lambda row: calcular_BUS(row, pesos_BUS), axis=1)
    columnas_finales = ["SERIE", "FECHA", "BUS"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

# =============================
# TABLAS FINALES
# =============================
df_BUS_detalles = get_df_detalles_ext_BUS()
df_BUS = df_BUS_detalles[["SERIE","FECHA","BUS"]]
df_BUS = df_BUS.rename(columns={"FECHA DE MUESTRA": "FECHA"})

# def get_df_extendida_BUS():
#     return df_BUS


def get_df_extendida_BUS():
    return df_BUS

__all__ = ["get_df_extendida_BUS"]

print(get_df_extendida_BUS())