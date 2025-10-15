import pandas as pd
import psycopg2
import pymysql

detalles = pd.read_excel(r'C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD_V2\Basededatos\detalles_transformadores.xlsx')

import pandas as pd
import pymysql

try:
    connection = pymysql.connect(
        host='vpce-01b25de29c6016b7e-i5efd99f.vpce-svc-074a6d8334ec25452.us-east-1.vpce.amazonaws.com',
        user='USU_SS_RO_ROQUISPEC',
        password='9Y8cE%wjd_',
        database='CDC',
        port=3306
    )
    print("✅ Conexión a SingleStore establecida correctamente")
    
    # CONSULTA SQL PARA EXTRAER DATOS
    consulta_sql = """
SELECT cod_scada, fecha_horita, VEX
FROM (
    SELECT 
        cod_scada,
        DATE(fecha_hora) AS fecha_horita, 
        SUM(comparacion) / 96 AS comparacion_sum,
        SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) AS comparacion_acumulada,
        SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 AS vex_acumulado,
        CASE 
            WHEN SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 100 THEN 5
            WHEN SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 75 THEN 4
            WHEN SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 50 THEN 3
            WHEN SUM(SUM(comparacion) / 96) OVER (PARTITION BY cod_scada ORDER BY DATE(fecha_hora)) / (40 * 365) * 100 > 25 THEN 2
            ELSE 1
        END AS VEX
    FROM (
        SELECT 
            cod_scada, 
            celda, 
            fecha_hora, 
            frecuencia, 
            pot_aparente, 
            tens_prom_s,
            (tens_prom_s / 210) * (60 / frecuencia) * 100 AS lado_izquierdo,
            110 - 5 * pot_aparente / 60 AS lado_derecho,
            CASE 
                WHEN (tens_prom_s / 210) * (60 / frecuencia) * 100 > (110 - 5 * pot_aparente / 60) THEN 1
                ELSE 0
            END AS comparacion
        FROM scada_lectura
        WHERE celda IN ('TRA1', 'TRA2', 'TRA3', 'TRA4')
          AND cod_scada LIKE '%/220/%'
          AND frecuencia IS NOT NULL AND frecuencia != 0
          AND pot_aparente IS NOT NULL AND pot_aparente != 0
          AND tens_prom_s IS NOT NULL AND tens_prom_s != 0
    ) AS base
    GROUP BY cod_scada, DATE(fecha_hora)
) AS resultado
ORDER BY fecha_horita DESC, cod_scada ASC;
    """
    
    # Ejecutar consulta y cargar en DataFrame
    df = pd.read_sql(consulta_sql, connection)
    print(f"✅ Datos extraídos: {len(df)} registros")
    
    # Mostrar primeras filas
    print(df.head())
    
    # Guardar como CSV si necesitas
    # df.to_csv('datos_extraidos.csv', index=False)
    
except pymysql.Error as e:
    print(f"❌ Error de conexión a SingleStore: {e}")
    
except Exception as e:
    print(f"❌ Error inesperado: {e}")
    
finally:
    if 'connection' in locals() and connection.open:
        connection.close()
        print("🔒 Conexión cerrada")


df = df.rename(columns={'fecha_horita': 'FECHA'})
df

# Separar cod_scada en SUBESTACION, TENSION y CIRCUITO
df[['SUBESTACION', 'TENSION', 'CIRCUITO']] = df['cod_scada'].str.split('/', expand=True)

# Reordenar columnas si lo deseas
df = df[['cod_scada', 'SUBESTACION', 'TENSION', 'CIRCUITO', 'FECHA', 'VEX']]
df = df.drop(columns=['cod_scada'])

subestaciones = df['SUBESTACION'].unique().tolist()
subestaciones

mapeo_subestaciones = {
    'ASIA': 'Asia',
    'BALNEARI': 'Balnearios',
    'CANTERA': 'Cantera',
    'MANCHAY': 'Manchay',
    'PACHACUT': 'Pachacútec',
    'PROGRESO': 'Progreso',
    'SANJUAN': 'San Juan',
    'SANLUIS': 'San Luis',
    'SROSAN': 'Santa Rosa Nueva'
}

df['SUBESTACION'] = df['SUBESTACION'].map(mapeo_subestaciones)

df= df[['SUBESTACION','CIRCUITO','TENSION','FECHA','VEX']]

# Unir los DataFrames por SUBESTACION y CIRCUITO
df_resultado = pd.merge(df, detalles, 
                        on=['SUBESTACION', 'CIRCUITO'], how='inner')

# Seleccionar solo las columnas deseadas
df_final = df_resultado[['SERIE', 'FECHA', 'VEX']]

print(df_final)

