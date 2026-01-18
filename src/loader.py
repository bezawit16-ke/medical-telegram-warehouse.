import os
import psycopg2
import json
from dotenv import load_dotenv

# Load database credentials from your .env file
load_dotenv()

def load_data():
    try:
        # 1. Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        cur = conn.cursor()

        # 2. Create a "raw" schema and a table for your messages
        print("Preparing database table...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_data (
                id SERIAL PRIMARY KEY,
                channel_title TEXT,
                channel_username TEXT,
                message_text TEXT,
                message_date TIMESTAMP,
                media_path TEXT
            );
        """)

        # 3. Path to your scraped JSON files
        # Adjust this path if your JSON files are in a different folder!
        data_path = 'data/raw/telegram_messages/'
        
        if not os.path.exists(data_path):
            print(f"Error: The folder {data_path} does not exist. Check your Task 1 output.")
            return

        # 4. Loop through JSON files and insert into the database
        for filename in os.listdir(data_path):
            if filename.endswith('.json'):
                print(f"Loading {filename}...")
                with open(os.path.join(data_path, filename), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for msg in data:
                        cur.execute("""
                            INSERT INTO raw.telegram_data (
                                channel_title, channel_username, message_text, message_date, media_path
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            msg.get('channel_title'), 
                            msg.get('channel_username'), 
                            msg.get('message'), 
                            msg.get('date'), 
                            msg.get('media_path')
                        ))

        conn.commit()
        print("Success! All data has been loaded into the medical_warehouse.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    load_data()