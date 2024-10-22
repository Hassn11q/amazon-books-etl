from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import timedelta

# Constants
POSTGRES_CONN_ID = "postgres_default"
HEADERS = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}
BASE_URL = "https://www.amazon.com/s?k=data+engineering+books"

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="amazon_books_etl",
    default_args=default_args,
    description="ETL pipeline to extract, transform, and load Amazon book data into Postgres",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    @task()
    def extract_amazon_data(num_books: int):
        """
        Extract book data from Amazon by scraping the website.
        """
        books = []
        seen_titles = set()
        page = 1

        while len(books) < num_books:
            url = f"{BASE_URL}&page={page}"
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                book_containers = soup.find_all("div", {"class": "s-result-item"})

                for book in book_containers:
                    title = book.find("span", {"class": "a-text-normal"})
                    author = book.find("a", {"class": "a-size-base"})
                    price = book.find("span", {"class": "a-price-whole"})
                    rating = book.find("span", {"class": "a-icon-alt"})

                    if title and author and price and rating:
                        book_title = title.text.strip()
                        if book_title not in seen_titles:
                            seen_titles.add(book_title)
                            books.append({
                                "title": book_title,
                                "author": author.text.strip(),
                                "price": price.text.strip(),
                                "rating": rating.text.strip()
                            })

                page += 1
            else:
                raise Exception(f"Request failed with status code {response.status_code}")

        # Return extracted books data for transformation
        return books

    @task()
    def transform_amazon_data(books):
        """
        Transform the extracted Amazon data into a cleaned DataFrame.
        """
        if not books:
            raise ValueError("No book data found.")

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(books)

        # Remove duplicates based on title column
        df.drop_duplicates(subset="title", inplace=True)

        # Return transformed data for loading
        return df.to_dict("records")

    @task()
    def load_amazon_data_to_postgres(transformed_books):
        """
        Load the cleaned book data into a Postgres database.
        """
        if not transformed_books:
            raise ValueError("No transformed book data found.")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                author TEXT,
                price TEXT,
                rating TEXT
            );
        """)

        # Insert data into the books table
        insert_query = """
            INSERT INTO books (title, author, price, rating)
            VALUES (%s, %s, %s, %s);
        """
        for book in transformed_books:
            cursor.execute(insert_query, (book['title'], book['author'], book['price'], book['rating']))

        conn.commit()
        cursor.close()
        conn.close()

    # Task flow using Taskflow API
    amazon_data = extract_amazon_data(num_books=10)
    transformed_data = transform_amazon_data(amazon_data)
    load_amazon_data_to_postgres(transformed_data)
