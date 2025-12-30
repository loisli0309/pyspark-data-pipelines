import openpyxl 
import pandas as pd 


df=pd.read_excel('data/Online Retail 80k.xlsx'  )
# print(df.head())
# print(df.shape)
# print(df.columns)
# print(df.dtypes)
# print(df.describe())


print(f"# of unique customers ids: {df['CustomerID'].unique()} ")

df_country_by_customerid= df.groupby('CustomerID')['Country'].unique().reset_index() 
print(df_country_by_customerid )
df_customerid_by_country = df.groupby('Country')['CustomerID'].unique().reset_index() 
print(df_customerid_by_country ) 