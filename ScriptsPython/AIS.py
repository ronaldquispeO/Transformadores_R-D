import pandas as pd
from functools import reduce
from FP import get_df_FP, get_df_extendida_FP
from CD import get_df_CD, get_df_extendida_CD
from FURANOS import get_df_FUR, get_df_extendida_FUR
from ECC import get_df_ECC, get_df_extendida_ECC

# =============================
# CONFIGURACIÓN
# =============================
PESOS_AIS = {
    'V': 1,
    'ECC': 5,
    'FUR': 2,
    'FPDEVANADO': 2,
    'CD': 3
}

# =============================
# FUNCIONES COMPARTIDAS
# =============================
def calcular_AIS(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

def procesar_dataframe(resultado, tablas):
    """Procesamiento común para todos los dataframes"""
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)
    
    # Calcular AIS
    resultado["AIS"] = resultado.apply(lambda row: calcular_AIS(row, PESOS_AIS), axis=1)
    
    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "AIS"] + list(tablas.keys())
    return resultado[columnas_finales]

def obtener_tablas(fp_func, cd_func, fur_func, ecc_func):
    """Obtiene y filtra las tablas disponibles"""
    tablas = {
        "FPDEVANADO": fp_func(),
        "CD": cd_func(),
        "FUR": fur_func(),
        "ECC": ecc_func()
    }
    return {k: v for k, v in tablas.items() if v is not None}

# =============================
# FUNCIONES PRINCIPALES
# =============================
def get_df_detalles_AIS():
    tablas = obtener_tablas(get_df_FP, get_df_CD, get_df_FUR, get_df_ECC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_ext_AIS():
    tablas = obtener_tablas(get_df_extendida_FP, get_df_extendida_CD, get_df_extendida_FUR, get_df_extendida_ECC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    return procesar_dataframe(resultado, tablas)

def get_df_detalles_rellenado_AIS():
    tablas = obtener_tablas(get_df_FP, get_df_CD, get_df_FUR, get_df_ECC)
    if not tablas:
        return pd.DataFrame()
    
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # Rellenar parámetros antes de calcular AIS
    for columna in tablas.keys():
        if columna in resultado.columns:
            resultado[columna] = resultado.groupby("SERIE")[columna].ffill()

    resultado["AIS"] = resultado.apply(lambda row: calcular_AIS(row, PESOS_AIS), axis=1)
    
    columnas_finales = ["SERIE", "FECHA", "AIS"] + list(tablas.keys())
    return resultado[columnas_finales]

# =============================
# DATAFRAMES FINALES
# =============================
df_AIS_detalles = get_df_detalles_ext_AIS()
df_AIS = df_AIS_detalles[['SERIE','FECHA','AIS']]
df_full_detallado = get_df_detalles_AIS()
df_full = df_full_detallado[['SERIE','FECHA','AIS']]

def get_df_AIS():
    return df_full

def get_df_extendida_AIS():
    return df_AIS

# =============================
# PRUEBAS
# =============================
df_arr = get_df_detalles_AIS()
df_arr_detalles = df_arr[['SERIE','FECHA','AIS']]

print(get_df_detalles_rellenado_AIS()[get_df_detalles_rellenado_AIS()['SERIE'] == '123158T'])
print(df_full[df_full['SERIE'] =='LP-000475'])