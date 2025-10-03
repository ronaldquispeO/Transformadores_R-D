import pandas as pd
import numpy as np

from NUCrnuc import get_df_RNUC, get_df_extendida_RNUC, get_df_detalles_RNUC, get_df_detalles_ext_RNUC
from NUCiex import get_df_IEX, get_df_extendida_IEX, get_df_detalles_IEX, get_df_detalles_ext_IEX

from functools import reduce

# =============================
# PESOS DE LOS ÍNDICES
# =============================

pesos_NUC = {
    'RNUC': 3,
    'IEX': 5,
    # 'VEX': 3,  # Agregar cuando esté disponible
}

# =============================
# FUNCIÓN PARA CALCULAR NUC
# =============================
def calcular_NUC(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

# =============================
# FUNCIONES PRINCIPALES
# =============================
def get_df_detalles_NUC():
    RNUC = get_df_RNUC()
    IEX = get_df_IEX()
    tablas = {
        "RNUC": RNUC,
        "IEX": IEX,
        # "VEX": VEX,  # Agregar cuando esté disponible
    }
    tablas = {k: v for k, v in tablas.items() if v is not None}
    if not tablas:
        return pd.DataFrame()
    from functools import reduce
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    resultado["NUC"] = resultado.apply(lambda row: calcular_NUC(row, pesos_NUC), axis=1)
    columnas_finales = ["SERIE", "FECHA", "NUC"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

def get_df_detalles_ext_NUC():
    RNUC = get_df_detalles_ext_RNUC()
    IEX = get_df_detalles_ext_IEX()
    tablas = {
        "RNUC": RNUC,
        "IEX": IEX,
        # "VEX": VEX,  # Agregar cuando esté disponible
    }
    tablas = {k: v for k, v in tablas.items() if v is not None}
    if not tablas:
        return pd.DataFrame()
    from functools import reduce
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    resultado["NUC"] = resultado.apply(lambda row: calcular_NUC(row, pesos_NUC), axis=1)
    columnas_finales = ["SERIE", "FECHA", "NUC"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    return resultado

df_NUC_detalles = get_df_detalles_ext_NUC()
df_NUC = df_NUC_detalles[["SERIE", "FECHA", "NUC"]]
df_NUC = df_NUC.rename(columns={"FECHA": "FECHA DE MUESTRA"})

def get_df_extendida_NUC():
    return df_NUC

print(get_df_detalles_NUC().head())
print(get_df_detalles_ext_NUC())
print(df_NUC)