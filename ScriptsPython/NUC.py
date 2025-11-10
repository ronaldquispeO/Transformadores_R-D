import pandas as pd
from functools import reduce
from NUCrnuc import get_df_RNUC, get_df_detalles_ext_RNUC
from NUCiex import get_df_IEX, get_df_detalles_ext_IEX

# =============================
# CONFIGURACIÓN
# =============================
PESOS_NUC = {
    'RNUC': 3,
    'IEX': 5,
    # 'VEX': 3,  # Agregar cuando esté disponible
}

# =============================
# FUNCIONES COMPARTIDAS
# =============================
def calcular_NUC(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

def procesar_dataframe(resultado, tablas):
    """Procesamiento común para todos los dataframes"""
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    resultado["NUC"] = resultado.apply(lambda row: calcular_NUC(row, PESOS_NUC), axis=1)
    columnas_finales = ["SERIE", "FECHA", "NUC"] + list(tablas.keys())
    return resultado[columnas_finales]

def obtener_tablas(rnuc_func, iex_func):
    """Obtiene y filtra las tablas disponibles"""
    tablas = {
        "RNUC": rnuc_func(),
        "IEX": iex_func(),
        # "VEX": VEX,  # Agregar cuando esté disponible
    }
    return {k: v for k, v in tablas.items() if v is not None}

# =============================
# FUNCIONES PRINCIPALES
# =============================
def get_df_detalles_NUC():
    tablas = obtener_tablas(get_df_RNUC, get_df_IEX)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_ext_NUC():
    tablas = obtener_tablas(get_df_detalles_ext_RNUC, get_df_detalles_ext_IEX)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_rellenado_NUC():
    tablas = obtener_tablas(get_df_RNUC, get_df_IEX)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # Rellenar parámetros antes de calcular NUC
    for columna in tablas.keys():
        if columna in resultado.columns:
            resultado[columna] = resultado.groupby("SERIE")[columna].ffill()

    resultado["NUC"] = resultado.apply(lambda row: calcular_NUC(row, PESOS_NUC), axis=1)
    columnas_finales = ["SERIE", "FECHA", "NUC"] + list(tablas.keys())
    return resultado[columnas_finales]

# =============================
# DATAFRAMES FINALES
# =============================
df_NUC_detalles = get_df_detalles_ext_NUC()
df_NUC = df_NUC_detalles[["SERIE", "FECHA", "NUC"]]
df_NUC = df_NUC.rename(columns={"FECHA DE MUESTRA": "FECHA"})
df_NUC_detallado = get_df_detalles_NUC()
df_full = df_NUC_detallado[["SERIE", "FECHA", "NUC"]]

def get_df_NUC():
    return df_full

def get_df_extendida_NUC():
    return df_NUC

# =============================
# PRUEBAS
# =============================
print(get_df_detalles_rellenado_NUC().head())