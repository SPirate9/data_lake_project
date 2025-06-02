import os
import shutil
from datetime import datetime, timedelta

DATA_LAKE_PATH = os.path.expanduser('~/data_lake_project/data_lake')
THRESHOLD_DAYS = 30

def clean_old_data():
    threshold_date = datetime.now().date() - timedelta(days=THRESHOLD_DAYS)
    for date_folder in os.listdir(DATA_LAKE_PATH):
        try:
            folder_date = datetime.strptime(date_folder, '%Y-%m-%d').date()
            if folder_date < threshold_date:
                shutil.rmtree(os.path.join(DATA_LAKE_PATH, date_folder))
                print(f"Deleted: {date_folder}")
        except ValueError:
            print(f"Skipping non-date folder: {date_folder}")

if __name__ == "__main__":
    clean_old_data()