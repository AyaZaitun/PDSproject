import multiprocessing
import json 
import pandas as pd
import requests
from sklearn.preprocessing import LabelEncoder,StandardScaler,normalize
from PIL import Image
from io import BytesIO
import time

df=pd.read_csv("imdb_top_1000.csv")
#identify the columns , null values and dataframe shape
print(df.columns)
print(df.head())
print(df.shape)
print(df.info())
print(df.describe())
print(df.isnull().sum())

del df["Overview"]

df["Gross"]=df["Gross"].str.replace(",","").astype(float)
df["Gross"]=df["Gross"].fillna(df["Gross"].mean()).astype(int)
df["Certificate"]=df["Certificate"].fillna(df["Certificate"].mode()[0])
df["Meta_score"]=df["Meta_score"].fillna(df["Meta_score"].median())
print(df.isnull().sum())

print(df.duplicated().sum())

groupby_director=df.groupby("Director").agg({"Gross":"sum"})
print(groupby_director)

groupby_gener=df.groupby("Genre").agg({"Movie_Title":"count"})
print(groupby_gener)

groupby_released_year=df.groupby("Released_Year").agg({"Movie_Title":"count"})
print(groupby_released_year)

scaler= StandardScaler()
df["standard_No_of_Votes"]=scaler.fit_transform(df.loc[:,["No_of_Votes"]])

df["norm_gross"]=normalize(df.loc[:,["Gross"]],axis=0,norm="l1")

print (df.groupby('Certificate').agg({"Certificate":"count"}))

label_encoder = LabelEncoder()
df['Certificate_Encoded'] = label_encoder.fit_transform(df['Certificate'])

print(df)

def open_url_poster_link(url_col):
    for i in url_col[0:5]:
        response = requests.get(i)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            img.show() 
            time.sleep(3)
        else:
            print(f"Failed to load image: {i}")
    
def movie_information(info):
    for i in info[0:5].itertuples(index=False):
        print(i)
        time.sleep(3)

if __name__ == "__main__":
    def1=multiprocessing.Process(target=open_url_poster_link,args=(df["Poster_Link"],))
    def2=multiprocessing.Process(target=movie_information,args=(df[["Movie_Title","Released_Year","Runtime","Genre","IMDB_Rating","Director"]],))
    
    def1.start()
    def2.start()
    
    def1.join()
    def2.join()
    print("finished")

file_name=open("IMDB_JSON.json",mode="r",encoding="utf-8")
data=json.load(file_name)
file_name.close()
data_json=pd.DataFrame(data)
df2=data_json.head(1000)
print(df2)

merge_data=pd.merge(df2,df,on="Movie_Title",how="inner")
print(merge_data)
merge_data.to_csv("mereged_imdb.csv")
