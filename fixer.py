import pandas as pd
import os

peers = pd.read_parquet("outputs/03_peer_groups.parquet")

print("Columns:", peers.columns.tolist())
print("\nPeer_Group_Size nulls:", peers["Peer_Group_Size"].isna().sum())
print("Confidence nulls:", peers["Confidence"].isna().sum())
print("\nSample:")
print(peers[["Flixbus_Row_ID", "Peer_Group_Size", "Confidence"]].head(10).to_string())