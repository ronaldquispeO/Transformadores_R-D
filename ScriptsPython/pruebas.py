import pandas as pd
import matplotlib.pyplot as plt
df  = pd.DataFrame({'A':[1,2,3],'B':[4,5,6]})  
print(df)


ax = plt.figure(8,12)

ax.plt(df.index,df.columns,color='blue',mark= 0)
plt.show()


