"""PostgreSQL database for handling deals"""
import psycopg2


class Database:
    """Database to keep the deals and results of algorithms"""
    def __init__(self):
        self.conn = psycopg2.connect(
                                    user="postgres",
                                    database="algorithm",
                                    password="user",
                                    port='5433',
                                    host='localhost',)

        self.cursor = self.conn.cursor()
        self.conn.autocommit = True
        if self.conn:
            self.clean_deals()
            print('Database connected!')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def flush(self):
        """Drops all the tables from database"""
        self.cursor.execute("DROP TABLE IF EXISTS orders")
        self.cursor.execute("DROP TABLE IF EXISTS deals")
        self.cursor.execute("DROP TABLE IF EXISTS results")
        self.cursor.execute("DROP TABLE IF EXISTS sizes")

    def create(self):
        """Creates the tables of database"""
        self.cursor.execute("CREATE TABLE IF NOT EXISTS "
                            "orders (ticker VARCHAR(16) PRIMARY KEY,"
                            "volume BIGINT)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS "
                            "deals (ticker VARCHAR(16),"
                            "price DECIMAL(8, 4),"
                            "volume INTEGER,"
                            "deal_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS "
                            "results (ticker VARCHAR(16),"
                            "vwap_algo DECIMAL(8, 4),"
                            "vwap_market DECIMAL(8, 4),"
                            "volume INTEGER,"
                            "end_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS "
                            "sizes (ticker VARCHAR(16) PRIMARY KEY,"
                            "deal_size INTEGER)")


    def get_deal_size(self, ticker: str) -> int:
        """Returns deal quantity by the ticker"""
        self.cursor.execute(
                            """SELECT deal_size
                                FROM sizes
                                WHERE ticker = %s""", ticker)
        return self.cursor.fetchone()

    def insert_deal(self, deal: dict) -> None:
        """Dumps the deal to the database"""
        ticker = deal['seccode']
        price = deal['price']
        volume = deal['qty']
        self.cursor.execute("""INSERT INTO deals(ticker, price, volume)
                               VALUES (%s, %s, %s)""", (ticker, price, volume))

    def calc_vwap(self, ticker) -> float:
        """Returns VWAP of the accomplished deals"""
        self.cursor.execute("""SELECT SUM(price * volume) / SUM(volume)
                               FROM deals
                               WHERE ticker = %s""", (ticker,))
        return self.cursor.fetchone()[0]

    def clean_deals(self) -> None:
        """Truncate deals table"""
        self.cursor.execute("TRUNCATE TABLE deals")

    def insert_results(self, ticker, vwap_market: float) -> None:
        """Dumps algorithm results to the database"""
        self.cursor.execute("""SELECT SUM(volume)
                                        FROM deals""")
        volume = self.cursor.fetchone()[0]
        vwap = self.calc_vwap(ticker)
        self.cursor.execute("""INSERT INTO results
                               VALUES (%s, %s, %s, %s)""",
                            (ticker, vwap, vwap_market, volume))


# database = Database()
# database.create()  # создание базы данных, если ее нет 
# database.flush()  # очистка всей базы данных

# print(database.calc_vwap())
