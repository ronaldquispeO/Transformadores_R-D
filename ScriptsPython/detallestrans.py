import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
address = r'C:\Users\roquispec\OneDrive - LUZ DEL SUR S.A.A\Documentos\Estudios de Ingreso\ProyectoRyD\BasedeDatos\detalles_transformadores.xlsx'

df = pd.read_excel(address)

try:
    # Codificar contraseña por si tiene caracteres especiales
    password = urllib.parse.quote_plus("delangeluz")
    
    # Crear engine de conexión
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/postgres')
    
    # Importar directamente el DataFrame a PostgreSQL
    # if_exists='replace' = borra la tabla si existe y crea una nueva
    # index=False = no incluir el índice del DataFrame como columna
    df.to_sql('bd_detalles', engine,schema='master_v2', if_exists='replace', index=False)
    
    print(f"¡Éxito! {len(df)} registros importados automáticamente")
    print("La tabla se creó con las columnas:", list(df.columns))
    
except Exception as e:
    print(f"Error: {e}")
print(df.head())
