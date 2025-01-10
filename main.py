import pandas as pd
from fastapi import FastAPI

app = FastAPI()

data=pd.read_csv(r"C:\Users\Dell\OneDrive\Desktop\PDSproject\mereged_imdb.csv")

@app.get("/")
def read_root():
    return data.to_dict()

@app.get("/Movie_Title/{Movie_Title}")
def movie_info(Movie_Title: str):
    filtered_data = data[data["Movie_Title"] == Movie_Title]
    return filtered_data.to_dict()
