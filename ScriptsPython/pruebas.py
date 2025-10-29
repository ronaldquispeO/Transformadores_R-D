import pandas as pd


df = pd.DataFrame({"SERIE":["AB123","AB 321"],"NUMERO":[1,2]})  # Suponiendo que df ya está definido en algún lugar
print(df)

    
df['SERIE'] = df['SERIE'].astype(str).str.replace(" ", "")
print(df)