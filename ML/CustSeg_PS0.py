import openpyxl
import pandas as pd

# df = pd.read_excel("C:\Users\Owner\Documents\GitProjects\spark\data\Online Retail 50000.xlsx")
# df.to_csv("C:\Users\Owner\Documents\GitProjects\spark\data\Online Retail 50000.csv", index=False)


from pathlib import Path
import pandas as pd

BASE_DIR = Path(r"C:\Users\Owner\Documents\GitProjects\spark")
data_dir = BASE_DIR / "data"

df = pd.read_excel(data_dir / "Online Retail 50000.xlsx")
df.to_csv(data_dir / "Online Retail 50000.csv", index=False)
