# Medical Telegram Data Warehouse (Final Submission)

An End-to-End Data Product that scrapes, transforms, and visualizes medical data from Telegram channels.

## 🚀 Features
- **Data Engineering:** Automated scraping of Telegram messages into PostgreSQL.
- **Analytics Engineering:** Star Schema implementation (Fact and Dimension tables) using **dbt**.
- **Data Product:** A **FastAPI** backend to serve data and a **Streamlit** dashboard for visualization.

## 🏗️ Project Structure
- `/api`: FastAPI implementation for data access.
- `/dashboard`: Streamlit code for the user interface.
- `/medical_transformation`: dbt models (stg, fct, dim).
- `/scripts`: Original scraper and loader scripts.

## 🛠️ How to Run
1. **Database:** Ensure PostgreSQL is running.
2. **API:** Run `uvicorn api.main:app --app-dir api --reload`
3. **Dashboard:** Run `streamlit run dashboard/app.py`