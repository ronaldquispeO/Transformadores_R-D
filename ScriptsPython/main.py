import sys
import pandas as pd
from functools import reduce

# Agregar la ruta donde están tus scripts DGA.py, ACE.py, etc.
sys.path.append(r"C:\Users\RONALD Q\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\ScriptsPython")

from DGA import get_df_extendida_DGA
from ACE import get_df_extendida_ACE
from ARR import get_df_extendida_ARR
from AIS import get_df_extendida_AIS
# from AIS import get_df_AIS
# from ARR import get_df_ARR

from NUC import get_df_extendida_NUC
from OLTC import get_df_extendida_OLTC
from BUS import get_df_extendida_BUS

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
# FUNCIÓN PRINCIPAL
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

    if not tablas:
        return pd.DataFrame()

    # Merge progresivo de todas las tablas (asumimos que todas ya tienen SERIE + FECHA DE MUESTRA extendidos)
    resultado = reduce(
        lambda left, right: pd.merge(left, right, on=["SERIE", "FECHA DE MUESTRA"], how="outer"),
        tablas.values()
    )

    # Asegurar tipo datetime en FECHA DE MUESTRA
    resultado["FECHA DE MUESTRA"] = pd.to_datetime(resultado["FECHA DE MUESTRA"], errors="coerce")

    # Ordenamos por SERIE y FECHA DE MUESTRA
    resultado = resultado.sort_values(by=["SERIE", "FECHA DE MUESTRA"]).reset_index(drop=True)

    # ============================
    # Calcular HI
    # ============================
    resultado["HI"] = resultado.apply(lambda row: calcular_HI(row, pesos_hi), axis=1)

    # Selección de columnas finales
    columnas_finales = ["SERIE", "FECHA DE MUESTRA", "HI"] + [col for col in tablas.keys()]
    resultado = resultado[columnas_finales]
    
    return resultado


# =============================
# EXPORTAR DATAFRAMES A POWER BI
# =============================

# Tabla principal de índice de salud
df_HI = obtener_HI()
dataset_HI = df_HI   # 👉 Power BI la reconocerá como tabla BD_IndiceSalud

# Tablas detalle (llamando directo a sus funciones si quieres verlas por separado)
# dataset_DGA = get_df_DGA()
# dataset_ACE = get_df_ACE()
print(df_HI)
# print(df_HI[df_HI['SERIE']==146916]) prueba para unico valor en ROHM
# Exportar CSV (opcional)
# df_HI.to_csv("dataset_HI.csv", index=False, encoding="utf-8-sig")
