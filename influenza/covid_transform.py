import pandas as pd

df = pd.DataFrame()
with open("COVID-19.csv", "w") as f:
    for line in f:
        line.replace("Korea, South", "South Korea")
        df = df.append(pd.read)
