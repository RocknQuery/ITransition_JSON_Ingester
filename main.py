import os.path
import re
import oracledb
import keyring
import getpass
import json
from pathlib import Path

class Parser:
    def __init__(self, file_path=r"C:\Users\User\Desktop\ITransition\Learning\Task_1\task1_d.json"):
        self.file_path = Path(file_path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")


    def get_validated_json(self) -> json:
        raw_text = Path(self.file_path).read_text(encoding="utf-8")
        validated_text = re.sub(r":(\w+)=>", r'"\1":', raw_text)

        try:
            validated_json = json.loads(validated_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON is not valid: {e}")

        # file_path_validated = self.file_path.with_name(self.file_path.stem + "_validated.json")
        # file_path_validated.write_text(validated_text, encoding="utf-8")
        return validated_json



class OracleDB:
    def __init__(self):
        self.service_name = "ORACLE_DB"
        self.user = "system"

        password = keyring.get_password(self.service_name, self.user)
        if password is None:
            password = getpass.getpass("Enter DB password: ")
            keyring.set_password(self.service_name, self.user, password)
        self.password = password
        self.dsn = "localhost:1521/orcl"


    def connect(self):
        try:
            return oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn
            )
        except oracledb.Error as e:
            raise ConnectionError(f"Error connecting to Oracle Database: {e}")


    def insert_data(self, validated_json: json):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                rows = []
                for book in validated_json:
                    # book_price_raw = book.get("price")

                    book_id = str(book.get("id"))
                    book_title = book.get("title")
                    book_author = book.get("author")
                    book_genre = book.get("genre")
                    book_publisher = book.get("publisher")
                    book_year = int(book.get("year"))
                    book_price = book.get("price")
                    # book_price = float(book_price_raw[1:])
                    # book_currency = book_price_raw[0]
                    # if book_currency.startswith('€'):
                    #     book_currency = 'EUR'
                    # elif book_currency.startswith('$'):
                    #     book_currency = 'USD'
                    # else:
                    #     raise ValueError(f"Unknown currency: {book_currency}")

                    rows.append((
                        book_id,
                        book_title,
                        book_author,
                        book_genre,
                        book_publisher,
                        book_year,
                        book_price
                        # ,book_currency,
                    ))

                cur.executemany("""insert into tb_first_task_books
                                              (id, title, author, genre, publisher, year, price)
                                            values
                                              (:1, :2, :3, :4, :5, :6, :7)""", rows)

                conn.commit()
                print(f"Records inserted: {len(validated_json)}")
        except oracledb.Error as e:
            conn.rollback()
            raise ConnectionError(f"Error occurred while inserting data to DB: {e}")
        finally:
            conn.close()



if __name__ == "__main__":
    parser = Parser()
    validated_json = parser.get_validated_json()

    db = OracleDB()
    db.insert_data(validated_json)