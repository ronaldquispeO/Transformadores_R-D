"""
Tienes una lista de enteros donde cada número aparece dos veces, excepto uno que aparece solo una vez.
¿Cómo encontrar el número único en la lista de manera eficiente?

"""
lista = [1,1,3,4,5,3,4,4,3,4,5,5,2]

j = None
lista2 =[]
for i in lista:
    if i == j:
        continue
    
