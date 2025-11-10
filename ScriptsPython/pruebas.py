import pandas as pd

df  = pd.DataFrame({'A':[1,2,3],'B':[4,5,6]})  
print(df)

lista = ['A','B']

for i in lista:
    df[i + ' al cuadrado'] = df.apply(lambda row: row[i]**2,axis =1)

print(df)


