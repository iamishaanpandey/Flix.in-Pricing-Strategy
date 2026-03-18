import pandas as pd
df = pd.read_parquet("outputs/05_final_output.parquet")
print(df.shape)
print(df.columns.tolist())
print(df.head(3).to_string())