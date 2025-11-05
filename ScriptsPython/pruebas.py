import pandas as pd

# Creamos un DataFrame de ejemplo
df = pd.DataFrame({
    'Nombre': ['Ana', 'Luis', 'Carlos'],
    'Edad': [17, 22, 15]
})

# Función que determina si es mayor de edad
def es_mayor(edad):
    return 'Sí' if edad >= 18 else 'No'

# Aplicamos la función a la columna 'Edad'
df['Mayor de Edad'] = df['Edad'].apply(es_mayor)

print(df)