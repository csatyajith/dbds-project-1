import pandas as pd

expressions = pd.read_csv("/Users/csatyajith/Datasets/DBDS_proj/dbds/GSE71766_expr.csv", index_col=0)
targets = pd.read_csv("/Users/csatyajith/Datasets/DBDS_proj/dbds/GSE71766_targets.csv", index_col=False)
print(expressions.sample())
transposed_expr = expressions.transpose()
transposed_expr.index.rename('Probe', inplace=True)
print(transposed_expr.sample())
merged_df = pd.merge(left=targets, right=transposed_expr, how='right', right_on="Probe", left_on='geo_accession')
print(merged_df.sample())
df_json = merged_df.to_json(orient="records")
with open("combined_data.json", "w") as f:
    f.write(df_json)
""
