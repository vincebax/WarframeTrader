import pathlib
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import relativedelta

class DataProcessor:
    
    def __init__(self):
        DB_PATH = pathlib.Path(os.path.join(pathlib.Path(__file__).parent.parent, 'data/warframe_data.db'))
        con = sqlite3.connect(DB_PATH)

        sql_query = "SELECT * FROM statistics"

        self._df = pd.read_sql_query(sql_query, con)

        # for graphing purposes
        self._df['timestamp'] = pd.to_datetime(self._df['timestamp'])
        self._df.set_index('timestamp', inplace=True)

    def plot_column(self, slug, column):

        condition = self._df['item_id'] == slug
        item_df = self._df[condition]

        item_df.plot(kind='line', y=column, title=f'{column} for {slug} over time')
        plt.show()

    def get_dataframe(self):
        return self._df

    def add_lagged_features(self, num_lags, day_function):
        self._df["avg_price"] = pd.to_numeric(self._df["avg_price"], errors="coerce")
        self._df["avg_price"] = self._df["avg_price"].ffill().fillna(1e-8)
        self._df["avg_price"] = self._df["avg_price"].clip(lower=1e-8)

        grouped = self._df.groupby("item_id")

        for i in range(num_lags):
            day = day_function(i + 1)
            self._df[f"log_lag_t-{day}"] = grouped["avg_price"].transform(lambda x: np.log(x).shift(day))
            self._df[f"log_return_{day}d"] = grouped["avg_price"].transform(lambda x: np.log(x).diff(day))


    def add_ewma_features(self, num_features, day_function):
        grouped = self._df.groupby("item_id")
        for i in range(num_features):
            day = day_function(i + 1)
            self._df[f"ewma_{day}"] = grouped["avg_price"].transform(lambda x: np.log(x).ewm(span=day, adjust=False).mean())


    def add_volatility_features(self, num_features, day_function):
        grouped = self._df.groupby("item_id")
        for i in range(num_features):
            day = day_function(i + 1)
            self._df[f"vol_{day}"] = grouped[f"log_return_{day}d"].transform(lambda x: x.rolling(day).std())

    def add_prime_vault_features(self):

        vaulting_map = {}

        request = requests.get("https://wiki.warframe.com/w/Prime_Vault#Vaulted_Items")
        page_html = request.text

        soup = BeautifulSoup(page_html, 'html.parser')

        table = soup.select_one("table.article-table.lighttable")
        rows = table.find_all('tr')

        for row in rows[1:]:
            temp = row.text.strip().replace('\xa0', ' ').split('\n')
            row_data = [item for item in temp if item != '']

            previous_resurgence_date = datetime.strptime(row_data[2] if len(row_data) == 5 else row_data[1], "%Y-%m-%d")
            
            today = datetime.today()

            ESTIMATED_RESURGENCE_CYCLE = 300

            time_since_last_resurgence = today - previous_resurgence_date
            days_since_last_resurgence = time_since_last_resurgence.days
            resurgence_cycle_progress = days_since_last_resurgence / ESTIMATED_RESURGENCE_CYCLE
            vaulting_map[row_data[0].lower().replace(' ', '_')] = {
                "days_since_last_resurgence" : days_since_last_resurgence,
                "resurgence_cycle_progress" : resurgence_cycle_progress,
                "resurgence_risk" : min (1.0, resurgence_cycle_progress),
                "recent_resurgence" : 1 if days_since_last_resurgence < 40 else 0
            }

        prime_cols = ["days_since_last_resurgence", "resurgence_cycle_progress",
              "resurgence_risk", "recent_resurgence"]

        # initialize columns with NaN
        for col in prime_cols:
            self._df[col] = np.nan

        def get_prime_features(item_id):
            matched_key = next((k for k in vaulting_map.keys() if item_id.startswith(k)), None)
            if matched_key:
                return vaulting_map[matched_key]
            else:
                return {col: np.nan for col in prime_cols}
            
        self._df[prime_cols] = self._df["item_id"].apply(lambda x: pd.Series(get_prime_features(x)))

        self._df["is_prime"] = self._df["days_since_last_resurgence"].notna().astype(int)

    def add_engineered_features(self, day_function):
        self.add_lagged_features(3, day_function)
        self.add_ewma_features(3, day_function)
        self.add_volatility_features(3, day_function)
        self.add_prime_vault_features()
