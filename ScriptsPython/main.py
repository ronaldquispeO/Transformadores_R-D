import sys
import pandas as pd
from functools import reduce

# Agregar la ruta donde están tus scripts DGA.py, ACE.py, etc.
sys.path.append(r"C:\Users\RONALD Q\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\ScriptsPython")

from DGA import get_df_extendida_DGA,get_df_DGA
from ACE import get_df_extendida_ACE,get_df_ACE
from ARR import get_df_extendida_ARR,get_df_ARR,get_df_detalles_rellenado_ARR
from AIS import get_df_extendida_AIS,get_df_AIS,get_df_detalles_rellenado_AIS
from NUC import get_df_extendida_NUC,get_df_NUC,get_df_detalles_rellenado_NUC
from OLTC import get_df_extendida_OLTC,get_df_OLTC
from BUS import get_df_extendida_BUS,get_df_BUS,get_df_detalles_rellenado_BUS

# =============================
# PESOS DE LOS ÍNDICES
# =============================
pesos_hi = {
    'DGA': 5,
    'ACE': 2,
    'AIS': 5,
    'ARR': 4,
    'NUC': 2,
    'OLTC': 1,
    'BUS': 1
}

# =============================
# FUNCIÓN PARA CALCULAR HI
# =============================

def calcular_HI(row, pesos):
    valores = [row[col] * peso for col, peso in pesos.items() if col in row and pd.notna(row[col])]
    total_peso = sum(peso for col, peso in pesos.items() if col in row and pd.notna(row[col]))
    return sum(valores) / total_peso if total_peso > 0 else float("nan")

# =============================
# FUNCIÓN PRINCIPAL MODIFICADA (CON RELLENO HACIA ADELANTE)
# =============================
def obtener_HI_original():
    
    # Llamar a las funciones (ya extendidas)
    DGA = get_df_DGA()
    ACE = get_df_ACE()
    AIS = get_df_detalles_rellenado_AIS()
    ARR = get_df_detalles_rellenado_ARR()
    NUC = get_df_detalles_rellenado_NUC()
    OLTC = get_df_OLTC()
    BUS = get_df_detalles_rellenado_BUS()

    # Guardamos en un diccionario (solo los disponibles)
    tablas = {
        "DGA": DGA,
        "ACE": ACE,
        "ARR": ARR,
        "AIS": AIS,
        "NUC": NUC,
        "OLTC": OLTC,
        "BUS": BUS
    }

    # Filtramos los que no sean None
    tablas = {k: v for k, v in tablas.items() if v is not None}

    # DEBUG: Mostrar columnas de cada DataFrame antes del merge
    for nombre, df in tablas.items():
        print(f"[{nombre}] columnas: {list(df.columns)}")
        if 'FECHA' not in df.columns:
            print(f"⚠️ ERROR: El DataFrame '{nombre}' NO tiene columna 'FECHA'.")

    if not tablas:
        return pd.DataFrame()

    # Merge progresivo de todas las tablas
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA"], how="outer"),
        tablas.values()
    )

    # Asegurar tipo datetime en FECHA
    resultado["FECHA"] = pd.to_datetime(resultado["FECHA"], errors="coerce")

    # Ordenamos por SERIE y FECHA
    resultado = resultado.sort_values(by=["SERIE", "FECHA"]).reset_index(drop=True)

    # ============================
    # RELLENAR DATOS HACIA LA FECHA MÁS ACTUAL
    # ============================
    # Agrupamos por serie y rellenamos hacia adelante (hasta la fecha más actual)
    resultado = resultado.groupby("SERIE").apply(
        lambda group: group.ffill()
    ).reset_index(drop=True)

    # ============================
    # Calcular HI
    # ============================
    resultado["HI"] = resultado.apply(lambda row: calcular_HI(row, pesos_hi), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "HI"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    
    return resultado

# =============================
# FUNCIÓN PRINCIPAL EXTENDIDA (SIN CAMBIOS)
# =============================
def obtener_HI():
    
    # Llamar a las funciones (ya extendidas)
    DGA = get_df_extendida_DGA()
    ACE = get_df_extendida_ACE()
    AIS = get_df_extendida_AIS()
    ARR = get_df_extendida_ARR()
    NUC = get_df_extendida_NUC()
    OLTC = get_df_extendida_OLTC()
    BUS = get_df_extendida_BUS()

    # Guardamos en un diccionario (solo los disponibles)
    tablas = {
        "DGA": DGA,
        "ACE": ACE,
        "ARR": ARR,
        "AIS": AIS,
        "NUC": NUC,
        "OLTC": OLTC,
        "BUS": BUS
    }

    # Filtramos los que no sean None
    tablas = {k: v for k, v in tablas.items() if v is not None}

    # DEBUG: Mostrar columnas de cada DataFrame antes del merge
    for nombre, df in tablas.items():
        print(f"[{nombre}] columnas: {list(df.columns)}")
        if 'FECHA' not in df.columns:
            print(f"⚠️ ERROR: El DataFrame '{nombre}' NO tiene columna 'FECHA'.")

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
    # Calcular HI
    # ============================
    resultado["HI"] = resultado.apply(lambda row: calcular_HI(row, pesos_hi), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA", "HI"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    
    return resultado


# =============================
# EXPORTAR DATAFRAMES A POWER BI
# =============================

# Tabla principal de índice de salud
df_HI = obtener_HI()
df_HI_original = obtener_HI_original()

dataset_HI = df_HI   # 👉 Power BI la reconocerá como tabla BD_IndiceSalud
print(df_HI_original)

# df_HI_original.to_csv("HI_rellenado.csv", index=False)