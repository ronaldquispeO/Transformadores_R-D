import pandas as pd
from functools import reduce
from ARRrohm import get_df_extendida_ROHM, get_df_ROHM
from ARRrtra import get_df_extendida_RTRA, get_df_RTRA
from ARRdis import get_df_DIS, get_df_extendida_DIS

# =============================
# CONFIGURACIÓN
# =============================
PESOS_ARR = {
    'ROHM': 3,
    'RTRA': 5,
    'RDIS': 5,
}

# =============================
# FUNCIONES COMPARTIDAS
# =============================
def calcular_ARR(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

def procesar_dataframe(resultado, tablas):
    """Procesamiento común para todos los dataframes"""
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    
    # Calcular ARR
    resultado["ARR"] = resultado.apply(lambda row: calcular_ARR(row, PESOS_ARR), axis=1)
    
    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "ARR"] + list(tablas.keys())
    return resultado[columnas_finales]

def obtener_tablas(rohm_func, rtra_func, rdis_func):
    """Obtiene y filtra las tablas disponibles"""
    tablas = {
        "ROHM": rohm_func(),
        "RTRA": rtra_func(),
        "RDIS": rdis_func(),
    }
    return {k: v for k, v in tablas.items() if v is not None}

# =============================
# FUNCIONES PRINCIPALES
# =============================
def get_df_detalles_ARR():
    tablas = obtener_tablas(get_df_ROHM, get_df_RTRA, get_df_DIS)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_ext_ARR():
    tablas = obtener_tablas(get_df_extendida_ROHM, get_df_extendida_RTRA, get_df_extendida_DIS)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_rellenado_ARR():
    tablas = obtener_tablas(get_df_ROHM, get_df_RTRA, get_df_DIS)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # Rellenar parámetros antes de calcular ARR
    for columna in tablas.keys():
        if columna in resultado.columns:
            resultado[columna] = resultado.groupby("SERIE")[columna].ffill()

    resultado["ARR"] = resultado.apply(lambda row: calcular_ARR(row, PESOS_ARR), axis=1)
    
    columnas_finales = ["SERIE", "FECHA", "ARR"] + list(tablas.keys())
    return resultado[columnas_finales]

# =============================
# DATAFRAMES FINALES
# =============================
df_ARR_detalles = get_df_detalles_ext_ARR()
df_ARR = df_ARR_detalles[['SERIE','FECHA','ARR']]
df_full_detallado = get_df_detalles_ARR()
df_full = df_full_detallado[['SERIE','FECHA','ARR']]

def get_df_ARR():
    return df_full

def get_df_extendida_ARR():
    return df_ARR

# =============================
# PRUEBAS
# =============================
hola = get_df_detalles_ext_ARR()
print(df_full[df_full['SERIE'] =='LP-000475'])