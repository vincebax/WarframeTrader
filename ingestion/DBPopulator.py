import pathlib
import os

import datetime
import time
from tqdm import tqdm

import sqlite3

import requests

class DBPopulator:

    BASE_PATH = 'https://api.warframe.market'

    def __init__(self):
        DB_PATH = pathlib.Path(os.path.join(pathlib.Path(__file__).parent.parent, 'data/warframe_data.db'))
        self.con = sqlite3.connect(DB_PATH)
        self.cur = self.con.cursor()
        self.cur.executescript(
            '''
            -- 1. Metadata for every item in the game
            CREATE TABLE IF NOT EXISTS items (
                item_id TEXT PRIMARY KEY,    -- The url_name (e.g., 'glaive_prime_set')
                item_name TEXT NOT NULL,     -- The human-readable name
                thumb TEXT,                  -- URL to the item icon
                last_updated DATETIME        -- Tracking when you last scraped this specific item
            );

            -- 2. Historical price data for AI training (Regression)
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                avg_price REAL,
                median REAL,
                volume INTEGER,
                min_price REAL,
                max_price REAL,
                
                UNIQUE(item_id, timestamp), 
                FOREIGN KEY (item_id) REFERENCES items(item_id)
            );

            -- 3. Real-time listings for immediate flipping
            CREATE TABLE IF NOT EXISTS live_orders (
                order_id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                price INTEGER NOT NULL,
                quantity INTEGER,
                order_type TEXT,            -- 'buy' or 'sell'
                user_status TEXT,           -- 'ingame' or 'online'
                last_seen DATETIME,
                FOREIGN KEY (item_id) REFERENCES items(item_id)
            );

            CREATE INDEX IF NOT EXISTS idx_stats_item_id ON statistics(item_id);
            CREATE INDEX IF NOT EXISTS idx_orders_item_id ON live_orders(item_id);
            '''
        )

    def close(self):
        if self.con:
            self.con.close()

    def scrape_items(self):
        all_items_req = requests.get(f'{self.BASE_PATH}/v2/items')
        all_items = all_items_req.json()['data']

        sql_query = """
                INSERT OR REPLACE INTO items (item_id, item_name, thumb, last_updated)
                VALUES (:slug, :name, :thumb, :last_updated)
            """

        for item in all_items:
            data = {
                'slug': item['slug'],
                'name': item['i18n']['en']['name'],
                'thumb': item['i18n']['en']['thumb'],
                'last_updated': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            self.cur.execute(sql_query, data)
        self.con.commit()

    def scrape_statistics(self):
        sql_query = """
                INSERT OR IGNORE INTO statistics (
                    item_id, 
                    timestamp, 
                    avg_price, 
                    median, 
                    volume, 
                    min_price, 
                    max_price
                ) VALUES (
                    :slug, 
                    :timestamp, 
                    :avg_price, 
                    :median, 
                    :volume, 
                    :min_price, 
                    :max_price
                )
            """

        self.cur.execute("SELECT item_id FROM items")
        slugs = [row[0] for row in self.cur.fetchall()]

        for slug in tqdm(slugs, desc="Scraping Warframe Market", unit="item"):
            statistics_request = requests.get(f'{self.BASE_PATH}/v1/items/{slug}/statistics')
            month_statistics = statistics_request.json()['payload']['statistics_live']['90days']

            for day in month_statistics:

                data = {
                    'slug': slug,
                    'timestamp': day['datetime'],
                    'avg_price': day['avg_price'],
                    'median': day['median'],
                    'volume': day['volume'],
                    'min_price': day['min_price'],
                    'max_price': day['max_price']
                }

                self.cur.execute(sql_query, data)
                
            self.con.commit()
            time.sleep(0.34)

    def full_scrape(self):
        self.scrape_items()
        self.scrape_statistics()