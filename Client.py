# не работает CREATE с драйверами %. Вставка в строку идет с кавычками.


import psycopg2


class Table:
    def __init__(self, cursor, connection):
        self.cursor = cursor
        self.connection = connection

    def create_table(self, table_name, id_name='id'):
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}({id_name} SERIAL PRIMARY KEY);
            """)
        self.connection.commit()

    def add_column(self, table_name, col_name, col_type, const_and_ref):
        self.cursor.execute(f"""
            alter TABLE {table_name}
            add column {col_name} {col_type} {const_and_ref};
            """)
        self.connection.commit()

    def drop_table(self, table_name):
        self.cursor.execute(f"""
            DROP TABLE {table_name};
            """)
        self.connection.commit()

    def add_new_client(self, firstname, surname, ):
        self.cursor.execute("""
            INSERT INTO client (firstname, surname) VALUES (%s, %s)
            """, (firstname, surname))
        self.connection.commit()

    def add_phone_for_client(self, client_id, phone_number):
        self.cursor.execute("""
            INSERT INTO phone_number (number, client_id) VALUES (%s, %s)
            """, (phone_number, client_id))
        self.connection.commit()

    def add_mail_address(self, client_id, address):
        self.cursor.execute("""
            INSERT INTO mail_address (address, client_id) VALUES (%s, %s)
            """, (address, client_id))
        self.connection.commit()

    def update_data(self):
        print("Для изменения информации о клиенте пожалуйста введите команду: \n "
              "1 - изменить имя; 2 - изменить фамилию; 3 - изменить e-mail; 4 - изменить номер телефона\n")

        while True:
            command = int(input('введите команду: '))
            person_id = int(input('Введите ID клиента: '))
            if command == 1:
                name = input('Введите новое имя: ').capitalize()
                self.cursor.execute("""
                UPDATE client SET firstname = %s
                WHERE client_id = %s
                RETURNING firstname
                """, (name, person_id))
                print(self.cursor.fetchone())
                break
            elif command == 2:
                surname = input('Введите новую фамилию: ').capitalize()
                self.cursor.execute("""
                UPDATE client SET surname = %s
                WHERE client_id = %s
                RETURNING surname
                """, (surname, person_id))
                print(self.cursor.fetchone())
                break
            elif command == 3:
                mail_address = input('Введите новый e-mail: ')
                self.cursor.execute("""
                UPDATE mail_address SET address = %s
                WHERE client_id = %s
                RETURNING address
                """, (mail_address, person_id))
                print(self.cursor.fetchone())
                break
            elif command == 4:
                phone_number = input('Введите номер телефона без пробелов, старт с 8-ки: ')
                self.cursor.execute("""
                UPDATE phone_number SET "number" = %s
                WHERE client_id = %s
                RETURNING "number"
                """, (phone_number, person_id))
                print(self.cursor.fetchone())
                break

    def delete_phone(self, number):
        self.cursor.execute('DELETE FROM phone_number WHERE "number" = %s;', (number,))
        self.connection.commit()

    def delete_client(self, client_id):
        self.cursor.execute('DELETE FROM phone_number WHERE client_id = %s;', (client_id,))
        self.cursor.execute('DELETE FROM mail_address WHERE client_id = %s;', (client_id,))
        self.cursor.execute('DELETE FROM client WHERE client_id = %s;', (client_id,))
        self.connection.commit()

    def find_person(self):
        print("Для поиска информации о клиенте, пожалуйста, введите команду, где:\n "
              "1 - найти по имени; 2 - найти по фамилии; 3 - найти по e-mail; 4 - найти по номеру телефона\n")

        while True:
            command = int(input('введите команду: '))
            if command == 1:
                name = input('Введите имя: ').capitalize()
                self.cursor.execute("""
                SELECT firstname, surname FROM client WHERE firstname = %s 
                """, (name, ))
                print(self.cursor.fetchone())
                break
            elif command == 2:
                surname = input('Введите фамилию: ').capitalize()
                self.cursor.execute("""
                SELECT firstname, surname FROM client WHERE surname = %s 
                """, (surname,))
                print(self.cursor.fetchone())
                break
            elif command == 3:
                mail_address = input('Введите e-mail: ')
                self.cursor.execute("""
                SELECT firstname, surname FROM client c
                JOIN mail_address ma ON c.client_id = ma.client_id
                WHERE ma.address = %s
                GROUP BY firstname, surname
                """, (mail_address,))
                print(self.cursor.fetchone())
                break
            elif command == 4:
                phone_number = input('Введите номер телефона без пробелов, старт с 8-ки: ')
                self.cursor.execute("""
                SELECT firstname, surname FROM client c 
                JOIN phone_number pn ON c.client_id = pn.client_id 
                WHERE pn."number" = %s
                GROUP BY firstname, surname
                """, (phone_number,))
                print(self.cursor.fetchone())
                break


if __name__ == '__main__':
    conn = psycopg2.connect(database='client_base_2', user='postgres', password='rozibu1991')
    with conn.cursor() as cur:
        client = Table(cur, conn)
        # # удаление таблиц
        #
        # client.drop_table('mail_address')
        # client.drop_table('phone_number')
        # client.drop_table('client')
        #
        # # создание таблиц и добавление колонок
        #
        # client.create_table('client', 'client_id')
        # client.add_column('client', 'firstname', 'VARCHAR(50)', 'NOT NULL')
        # client.add_column('client', 'surname', 'VARCHAR(50)', 'NOT NULL')
        #
        # client_phone = Table(cur, conn)
        # client.create_table('phone_number', 'phone_id')
        # client.add_column('phone_number', 'number', 'BIGINT', '')
        # client.add_column('phone_number', 'client_id', 'INTEGER', 'NOT NULL REFERENCES client(client_id)')
        #
        # client_mail = Table(cur, conn)
        # client.create_table('mail_address', 'address_id')
        # client.add_column('mail_address', 'address', 'VARCHAR(70)', 'NOT NULL')
        # client.add_column('mail_address', 'client_id', 'INTEGER', 'NOT NULL REFERENCES client(client_id)')
        #
        # # наполнение таблиц
        # #
        # client.add_new_client('Morty', 'Smith')
        # client.add_new_client('Rick', 'Sanchez')
        # client.add_new_client('Jerry', 'Smith')
        #
        # client.add_phone_for_client(1, 89999999999)
        # client.add_phone_for_client(1, 89999955555)
        # client.add_phone_for_client(2, 89999444444)
        # client.add_phone_for_client(2, 89999333333)
        # client.add_phone_for_client(3, 89999555555)
        #
        # client.add_mail_address(1, 'morty_dog@mail.ru')
        # client.add_mail_address(2, 'god_damn_cool_rick@gmail.com')
        # client.add_mail_address(2, 'wubba_lubba_dub_dub@my-god-damn-own-mail.com')
        # client.add_mail_address(3, 'jerry_schmuck@gmail.com')

        # client.update_data()
        # client.delete_phone(89999999999)
        # client.delete_client(2)
        # client.find_person()

    conn.close()
