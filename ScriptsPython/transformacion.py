import pandas as pd
from DGA import BD_DGA
from ACE import BD_ACE
from ARRrohm import BD_ARRrohm
from ARRrtra import BD_ARRrtra
from main import obtener_HI
from sqlalchemy import create_engine
import urllib.parse


# Ruta al archivo CSV
# address = r'C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoR&D\Transformaciones\Excels\pedidos.csv'

# Leer el CSV
# df = pd.read_csv(address, sep=';', encoding='utf-8')
# print("Datos del CSV:")
# print(df.head())

# IMPORTAR DATAFRAME HI:

df_dga = BD_DGA()
df_ace = BD_ACE()
df_rohm = BD_ARRrohm()
df_rtra = BD_ARRrtra()
df_hi = obtener_HI()
print(df_hi.dtypes)
print(df_hi.head(3))

# Conexión con SQLAlchemy (MUCHO más simple)
try:
    # Codificar contraseña por si tiene caracteres especiales
    password = urllib.parse.quote_plus("delangeluz")
    
    # Crear engine de conexión
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/postgres')
    
    # Importar directamente el DataFrame a PostgreSQL
    # if_exists='replace' = borra la tabla si existe y crea una nueva
    # index=False = no incluir el índice del DataFrame como columna
    df_rtra.to_sql('rtra', engine, if_exists='replace', index=False)
    
    print(f"¡Éxito! {len(df_rtra)} registros importados automáticamente")
    print("La tabla se creó con las columnas:", list(df_rtra.columns))
    
except Exception as e:
    print(f"Error: {e}")