# %% [markdown]
# ### Importar las librerías

# %%
import pandas as pd
import singlestoredb as s2
import json
import numpy as np

detalles = pd.read_excel(r'C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\Basededatos\detalles_transformadores.xlsx')

# %% [markdown]
# ### Extracción de datos desde SingleStore

# %%
try:
    config_path = r"C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\notebooks\config.json"
    with s2.connect(**json.load(open(config_path))['singlestore']) as connection:
        
        consulta_sql = """
SELECT
    CASE
        WHEN s.cod_scada LIKE 'ASIA/%' THEN 'Asia'
        WHEN s.cod_scada LIKE 'ALTOPRAD/%' THEN 'Alto Pradera'
        WHEN s.cod_scada LIKE 'BALNEARI/%' THEN 'Balnearios'
        WHEN s.cod_scada LIKE 'SBARTOLO/%' THEN 'San Bartolo'
        WHEN s.cod_scada LIKE 'CANTERA/%' THEN 'Cantera'
        WHEN s.cod_scada LIKE 'MANCHAY/%' THEN 'Manchay'
        WHEN s.cod_scada LIKE 'PACHACUT/%' THEN 'Pachacutec'
        WHEN s.cod_scada LIKE 'PROGRESO/%' THEN 'Progreso'
        WHEN s.cod_scada LIKE 'SANJUAN/%' THEN 'San Juan'
        WHEN s.cod_scada LIKE 'SANLUIS/%' THEN 'San Luis'
        WHEN s.cod_scada LIKE 'SROSAN/%' THEN 'Santa Rosa Nueva'
        ELSE 'Desconocido'
    END AS nombre_subestacion,
    s.celda,
    s.fecha_hora,
    s.frecuencia,
    s.pot_aparente,
    s.tens_prom_s,
    t.tension_tap,
    t.pot_instalada,
    ROUND((s.tens_prom_s / t.tension_tap) * (60 / s.frecuencia) * 100, 2) AS lado_izquierdo,
    ROUND(110 - 5 * (s.pot_aparente / t.pot_instalada), 2) AS lado_derecho,
    CASE 
        WHEN ROUND((s.tens_prom_s / t.tension_tap) * (60 / s.frecuencia) * 100, 2) > 
             ROUND(110 - 5 * (s.pot_aparente / t.pot_instalada), 2)
        THEN 1 
        ELSE 0 
    END AS comparacion,
    s.cod_scada
FROM scada_lectura s
INNER JOIN (
    SELECT 'Alto Pradera' AS subestacion, 'TRF1' AS circuito, 65.345 AS tension_tap, 50.00 AS pot_instalada
    UNION SELECT 'Asia', 'TRA1', 210.000, 255.00
    UNION SELECT 'Balnearios', 'TRA2', 210.000, 120.00
    UNION SELECT 'Balnearios', 'TRA3', 200.000, 180.00
    UNION SELECT 'Balnearios', 'TRA4', 210.000, 180.00
    UNION SELECT 'Cantera', 'TRA1', 214.000, 25.00
    UNION SELECT 'Manchay', 'TRA1', 220.000, 120.00
    UNION SELECT 'Pachacutec', 'TRA2', 240.022, 50.00
    UNION SELECT 'Progreso', 'TRA1', 240.022, 50.00
    UNION SELECT 'San Bartolo', 'TRF2', 65.345, 25.00
    UNION SELECT 'San Juan', 'TRA1', 210.000, 180.00
    UNION SELECT 'San Juan', 'TRA2', 210.000, 180.00
    UNION SELECT 'San Luis', 'TRA2', 240.022, 50.00
    UNION SELECT 'Santa Rosa Nueva', 'TRA3', 210.000, 120.00
    UNION SELECT 'Santa Rosa Nueva', 'TRA4', 210.000, 120.00
) t ON 
    CASE
        WHEN s.cod_scada LIKE 'ASIA/%' THEN 'Asia'
        WHEN s.cod_scada LIKE 'ALTOPRAD/%' THEN 'Alto Pradera'
        WHEN s.cod_scada LIKE 'BALNEARI/%' THEN 'Balnearios'
        WHEN s.cod_scada LIKE 'SBARTOLO/%' THEN 'San Bartolo'
        WHEN s.cod_scada LIKE 'CANTERA/%' THEN 'Cantera'
        WHEN s.cod_scada LIKE 'MANCHAY/%' THEN 'Manchay'
        WHEN s.cod_scada LIKE 'PACHACUT/%' THEN 'Pachacutec'
        WHEN s.cod_scada LIKE 'PROGRESO/%' THEN 'Progreso'
        WHEN s.cod_scada LIKE 'SANJUAN/%' THEN 'San Juan'
        WHEN s.cod_scada LIKE 'SANLUIS/%' THEN 'San Luis'
        WHEN s.cod_scada LIKE 'SROSAN/%' THEN 'Santa Rosa Nueva'
        ELSE 'Desconocido'
    END = t.subestacion AND s.celda = t.circuito
WHERE s.celda IN ('TRA1','TRA2','TRA3','TRA4','TRA5','TRF1','TRF2','TRF3','TRF4','TRF5')
ORDER BY s.fecha_hora DESC;
"""
        df = pd.read_sql(consulta_sql, connection)
        
except Exception as e:
    print(f"❌ Error: {e}")

# %%
df = df.rename(columns={'nombre_subestacion': 'SUBESTACION','celda':'CIRCUITO'}).drop(columns=['cod_scada'])

# %%
df_resultado = pd.merge(df, 
                        detalles[['SUBESTACION', 'CIRCUITO', 'SERIE']], 
                        on=['SUBESTACION', 'CIRCUITO'], 
                        how='inner')
df_resultado["SERIE"] = df_resultado["SERIE"].astype(str).str.replace(" ", "")

# %% [markdown]
# ### Cálculo del índice VEX

# %%
# Convertir a datetime para evitar problemas de comparación
df_resultado['FECHA'] = pd.to_datetime(df_resultado['fecha_hora']).dt.date
df_resultado['FECHA'] = pd.to_datetime(df_resultado['FECHA'])  # Convertir a datetime64[ns]

df_resultado_agrupado = (df_resultado.groupby(['SERIE', 'FECHA'])['comparacion']
                         .sum()
                         .reset_index()
                         .rename(columns={'comparacion': 'suma_comparacion'}))

df_resultado_agrupado['suma_comparacion_96'] = df_resultado_agrupado['suma_comparacion'] / 96
df_resultado_agrupado = df_resultado_agrupado.sort_values(['SERIE', 'FECHA'])
df_resultado_agrupado['suma_acumulativa'] = df_resultado_agrupado.groupby('SERIE')['suma_comparacion_96'].cumsum()
df_resultado_agrupado['suma_acumulativa_14600'] = df_resultado_agrupado['suma_acumulativa'] / 14600

def asignar_puntaje_vex(vex):
    if vex > 1.00: return 5
    elif vex > 0.75: return 4
    elif vex > 0.50: return 3
    elif vex > 0.25: return 2
    else: return 1

df_resultado_agrupado['VEX'] = df_resultado_agrupado['suma_acumulativa_14600'].apply(asignar_puntaje_vex)

# =============================
# TABLAS FINALES Y FUNCIONES
# =============================

# 1. Tabla con fechas originales: SERIE, FECHA, VEX
df_VEX = df_resultado_agrupado[['SERIE', 'FECHA', 'VEX']].copy()

# 2. Tabla con fechas extendidas: SERIE, FECHA, VEX
inicio = "2015-01-01"
fecha_inicio = pd.Timestamp(inicio)
fecha_fin = pd.Timestamp.today().normalize()
fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
todas_series = df_resultado_agrupado['SERIE'].dropna().unique()
calendario = pd.MultiIndex.from_product([todas_series, fechas], names=["SERIE", "FECHA"])
df_calendario = pd.DataFrame(index=calendario).reset_index()

# CORRECCIÓN: Usar pd.Timestamp para comparación consistente
ultimos_2014 = df_VEX[df_VEX['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014['FECHA'] = fecha_inicio
base_ext = pd.concat([df_VEX, ultimos_2014], ignore_index=True)
df_VEX_ext = pd.merge(df_calendario, base_ext, on=["SERIE","FECHA"], how="left")
df_VEX_ext = df_VEX_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# 3. Tabla de detalles con fechas originales
# Primero asegurarnos que df_resultado tenga FECHA como datetime
df_resultado['FECHA'] = pd.to_datetime(df_resultado['FECHA'])

# Crear tabla de detalles con VEX después de FECHA
df_detalles = df_resultado[['SERIE', 'FECHA', 'frecuencia', 'pot_aparente', 'tens_prom_s', 'tension_tap', 
                           'pot_instalada', 'lado_izquierdo', 'lado_derecho', 'comparacion']].copy()
df_detalles = pd.merge(df_detalles, df_VEX, on=['SERIE', 'FECHA'], how='left')

# Reordenar columnas para que VEX quede después de FECHA
def reordenar_columnas(df):
    cols = list(df.columns)
    if "VEX" in cols:
        cols.remove("VEX")
        idx = cols.index("FECHA") + 1
        cols = cols[:idx] + ["VEX"] + cols[idx:]
    return df[cols]

df_detalles = reordenar_columnas(df_detalles)

# 4. Tabla de detalles con fechas extendidas
ultimos_2014_det = df_detalles[df_detalles['FECHA'] < fecha_inicio].sort_values('FECHA').groupby('SERIE').tail(1)
ultimos_2014_det['FECHA'] = fecha_inicio
base_ext_det = pd.concat([df_detalles, ultimos_2014_det], ignore_index=True)
df_detalles_ext = pd.merge(df_calendario, base_ext_det, on=["SERIE","FECHA"], how="left")
df_detalles_ext = df_detalles_ext.groupby("SERIE").apply(lambda g: g.ffill()).reset_index(drop=True)

# Asegurar que la tabla extendida también tenga VEX después de FECHA
df_detalles_ext = reordenar_columnas(df_detalles_ext)

# =============================
# FUNCIONES PARA LLAMAR
# =============================
def get_df_VEX():
    return df_VEX.copy()

def get_df_extendida_VEX():
    return df_VEX_ext.copy()

def get_df_detalles_VEX():
    return df_detalles.copy()

def get_df_detalles_ext_VEX():
    return df_detalles_ext.copy()

# =============================
# PRINT DE TABLAS
# =============================
print('\n ====== TABLA CON FECHAS ORIGINALES ====== \n')
print(get_df_VEX().head(20), '\n')

print('\n ====== TABLA CON FECHAS EXTENDIDAS ====== \n')
print(get_df_extendida_VEX().tail(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS ORIGINALES ====== \n')
print(get_df_detalles_VEX().head(), '\n')

print('\n ====== TABLA DE DETALLES CON FECHAS EXTENDIDAS ====== \n')
print(get_df_detalles_ext_VEX().head(), '\n')