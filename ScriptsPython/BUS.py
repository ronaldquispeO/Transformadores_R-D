import pandas as pd
from functools import reduce
from FPBC1 import get_df_FP_C1, get_df_extendida_FP_C1
from FPBC2 import get_df_FP_C2, get_df_extendida_FP_C2
from CBC1 import get_df_C_C1, get_df_extendida_C_C1
from CBC2 import get_df_C_C2, get_df_extendida_C_C2
from FPCOCA import get_df_CC, get_df_extendida_CC

# =============================
# CONFIGURACIÓN
# =============================
PESOS_BUS = {
    'FPBC1': 4,
    'FPBC2': 2,
    'CBC1': 5,
    'CBC2': 3,
    'CC': 3
}

# =============================
# FUNCIONES COMPARTIDAS
# =============================
def calcular_BUS(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

def procesar_dataframe(resultado, tablas):
    """Procesamiento común para todos los dataframes"""
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    resultado["BUS"] = resultado.apply(lambda row: calcular_BUS(row, PESOS_BUS), axis=1)
    columnas_finales = ["SERIE", "FECHA", "BUS"] + list(tablas.keys())
    return resultado[columnas_finales]

def obtener_tablas(fpbc1_func, fpbc2_func, cbc1_func, cbc2_func, cc_func):
    """Obtiene y filtra las tablas disponibles"""
    tablas = {
        "FPBC1": fpbc1_func(),
        "FPBC2": fpbc2_func(),
        "CBC1": cbc1_func(),
        "CBC2": cbc2_func(),
        "CC": cc_func(),
    }
    return {k: v for k, v in tablas.items() if v is not None}

# =============================
# FUNCIONES PRINCIPALES
# =============================
def get_df_detalles_BUS():
    tablas = obtener_tablas(get_df_FP_C1, get_df_FP_C2, get_df_C_C1, get_df_C_C2, get_df_CC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_ext_BUS():
    tablas = obtener_tablas(get_df_extendida_FP_C1, get_df_extendida_FP_C2, get_df_extendida_C_C1, get_df_extendida_C_C2, get_df_extendida_CC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_rellenado_BUS():
    tablas = obtener_tablas(get_df_FP_C1, get_df_FP_C2, get_df_C_C1, get_df_C_C2, get_df_CC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # Rellenar parámetros antes de calcular BUS
    for columna in tablas.keys():
        if columna in resultado.columns:
            resultado[columna] = resultado.groupby("SERIE")[columna].ffill()

    resultado["BUS"] = resultado.apply(lambda row: calcular_BUS(row, PESOS_BUS), axis=1)
    columnas_finales = ["SERIE", "FECHA", "BUS"] + list(tablas.keys())
    return resultado[columnas_finales]

# =============================
# TABLAS FINALES
# =============================
df_BUS_detalles = get_df_detalles_ext_BUS()
df_BUS = df_BUS_detalles[["SERIE","FECHA","BUS"]]
df_full_detallado = get_df_detalles_BUS()
df_full = df_full_detallado[["SERIE","FECHA","BUS"]]

def get_df_BUS():
    return df_full

def get_df_extendida_BUS():
    return df_BUS

# =============================
# PRUEBAS
# =============================
print(get_df_detalles_rellenado_BUS().tail())