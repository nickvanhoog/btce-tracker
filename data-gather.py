import logging
from time import time as time

import requests
import json
import MySQLdb as mysqldb

logging.basicConfig(filename="btc-data-gather.log", level=logging.ERROR)

class BTCDataGatherer():
    ltc_usd_urls = {
                'ticker': 'https://btc-e.com/api/2/ltc_usd/ticker',
                'trades': 'https://btc-e.com/api/2/ltc_usd/trades',
                'depth': 'https://btc-e.com/api/2/ltc_usd/depth' 
               }

    btc_usd_urls = {
                'ticker': 'https://btc-e.com/api/2/btc_usd/ticker',
                'trades': 'https://btc-e.com/api/2/btc_usd/trades',
                'depth': 'https://btc-e.com/api/2/btc_usd/depth' 
               }

    ltc_btc_urls = {
                'ticker': 'https://btc-e.com/api/2/ltc_btc/ticker',
                'trades': 'https://btc-e.com/api/2/ltc_btc/trades',
                'depth': 'https://btc-e.com/api/2/ltc_btc/depth' 
               }

    TRADES = 'trades'
    TICKER = 'ticker'
    DEPTH = 'depth'

    __ltc_usd_table = 'ltc_usd_trades'
    __ltc_btc_table = 'ltc_btc_trades'
    __btc_usd_table = 'btc_usd_trades'

    def __init__(self):
        self.last_request_time = 0

    def connect_to_db(self):
        """ Connects to the MySQL DB and creates a cursor to the DB. """
        self.db_conn = mysqldb.connect(host="localhost", user="BTC_admin", passwd="radiobulletin", db="BTC")
        self.db_cursor = self.db_conn.cursor()

    def block_for(self, wait_time):
        """ Blocks all activity until it's been wait_time seconds since the last API request batch. """
        while time() - self.last_request_time < wait_time:
            continue
        self.last_request_time = time()

    def write_trade_to_db(self, trade_dict, table_name):
        """ Inserts the trade information in trade_dict into the table with name table_name. """
        self.db_cursor.execute("""INSERT INTO {} (date, price, amount, tid, price_currency, item, trade_type) 
                                VALUES(FROM_UNIXTIME({date}), {price}, {amount}, {tid}, '{price_currency}', '{item}', '{trade_type}') 
                                ON DUPLICATE KEY UPDATE date=FROM_UNIXTIME({date})""".format(table_name, **trade_dict))

    def hit_trades_api(self, url, table_name):
        """ Puts data from the BTC-E API for the various markets into the MySQL database."""
        r = requests.get(url)
        json_data = json.loads(r.content) # json_data is now a list of each of the rows; json_data[i] is a dict

        for trade in json_data:
            self.write_trade_to_db(trade, table_name)

    def gather(self):
        """ Monitors the BTC-E trades API for each market and writes the
            data to files """

        # Initialize connection to the database
        self.connect_to_db()

        while True:
            try:
                if not self.db_conn.open:
                    self.connect_to_db()
                
                self.hit_trades_api(self.ltc_usd_urls[self.TRADES], self.__ltc_usd_table)
                self.hit_trades_api(self.btc_usd_urls[self.TRADES], self.__btc_usd_table)
                self.hit_trades_api(self.ltc_btc_urls[self.TRADES], self.__ltc_btc_table)

                # Commit DB changes
                self.db_conn.commit()
            except Exception as e:
                logging.error('***** Error: {} *****'.format(e.message))

            self.block_for(30)

        return 'Main gathering loop exited'

if __name__ == '__main__':
    dg = BTCDataGatherer()
    dg.gather()