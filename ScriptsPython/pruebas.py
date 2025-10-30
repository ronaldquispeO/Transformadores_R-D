import pandas as pd

# Crear un DataFrame de ejemplo
data = {
    'Nombre': ['Ana', 'Luis', 'Carlos'],
    'Edad': [23, 31, 45],
    'Ciudad': ['Lima', 'Cusco', 'Arequipa']
}

df = pd.DataFrame(data)

# Mostrar el DataFrame completo
print("DataFrame completo:\n")
print(df)

# Seleccionar la primera fila (índice 0)
print("\nPrimera fila:\n")
print(df.iloc[0:1])

# Seleccionar la segunda fila y la columna 'Edad'
print("\nSegunda fila, columna 'Edad':\n")
print(df.iloc[1, 1])

# Seleccionar las dos primeras filas
print("\nDos primeras filas:\n")
print(df.iloc[0:2])
