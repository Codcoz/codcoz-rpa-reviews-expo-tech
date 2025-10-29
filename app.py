import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# Passo 1: Login
numero_mapa = 9
login_url = "https://expo-tech-backend.onrender.com/users/login"
login_data = {
    "username": f"expositor_project-uuid-{numero_mapa}@example.com",
    "password": os.getenv("API_PASSWD")
}
login_headers = {
    "accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}
response = requests.post(login_url, data=login_data, headers=login_headers)
token = response.json().get("access_token")

# Passo 2: Buscar reviews do projeto
if token:
    reviews_url = "https://expo-tech-backend.onrender.com/reviews/project/"
    reviews_headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    reviews_response = requests.get(reviews_url, headers=reviews_headers)

    data = reviews_response.json()
    
    # Passo 3: Transformar dados em DataFrame para uso do BI
    all_grades = []
    for review in data:
        for grade in review["grades"]:
            all_grades.append({
                "review_id": review["id"],
                "grade_name": grade["name"],
                "score": grade["score"],
                "weight": grade["weight"],
            })
    df = pd.DataFrame(all_grades)

engine = create_engine(os.getenv("DB_URL"))

df = (
    df
    .set_index(['review_id', 'grade_name'])
    .unstack('grade_name')  # transforma critérios em colunas
)

df.columns = [f"{col[0]}_{col[1].lower().replace(' ', '_').replace('ã', 'a').replace('ç', 'c').replace('é', 'e')}"
                    for col in df.columns]

df = df.reset_index()

df.to_sql(
    "reviews_expo_tech", # nome tabela
    engine, 
    if_exists="replace", # fail, replace, append
    index=False # não coloca o índice
)