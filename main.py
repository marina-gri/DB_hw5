from pprint import pprint
import psycopg2


# 1. Функция, создающая структуру БД (таблицы).
def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
                DROP TABLE client_info CASCADE;
                DROP TABLE phones CASCADE;
                """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS client_info(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(40),
                last_name VARCHAR(40),
                email VARCHAR(40) UNIQUE                   
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones(
                phone_id SERIAL PRIMARY KEY,
                client_id INT NOT NULL,
                phone_number VARCHAR(12) UNIQUE NOT NULL,
                FOREIGN KEY (client_id) REFERENCES client_info(client_id)
                ON DELETE CASCADE
            );
        """)
        conn.commit()


# 2. Функция, позволяющая добавить нового клиента.
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
                    INSERT INTO client_info(first_name, last_name, email)
                    VALUES(%s, %s, %s) RETURNING client_id;
                    """, (first_name, last_name, email)
                    )
        cl_id = cur.fetchone()[0]

        if phones == None:
            pass

        elif type(phones) == tuple or type(phones) == list:
            for number in phones:
                cur.execute("""
                            INSERT INTO phones(client_id, phone_number)
                            VALUES(%s, %s);
                            """, (cl_id, number)
                            )
        else:
            cur.execute("""
                                        INSERT INTO phones(client_id, phone_number)
                                        VALUES(%s, %s);
                                        """, (cl_id, phones)
                        )
        conn.commit()


# 3. Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(conn, client_id, phones):
    with conn.cursor() as cur:
        if type(phones) == tuple or type(phones) == list:
            for number in phones:
                cur.execute("""
                            INSERT INTO phones(client_id, phone_number)
                            VALUES(%s, %s);
                            """, (client_id, number)
                            )
        else:
            cur.execute("""
                        INSERT INTO phones(client_id, phone_number)
                        VALUES(%s, %s);
                        """, (client_id, phones)
                        )
        conn.commit()


# 4. Функция, позволяющая изменить данные о клиенте.
def change_client(conn, client_id, first_name, last_name, email):
    with conn.cursor() as cur:
        cur.execute("""
                UPDATE client_info SET first_name=%s, last_name=%s, email=%s WHERE client_id=%s;
                """, (first_name, last_name, email, client_id))
        conn.commit()


# 5. Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM phones WHERE client_id=%s AND phone_number=%s;
                """, (client_id, phone))
        conn.commit()


# 6. Функция, позволяющая удалить существующего клиента.
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
                DELETE FROM client_info WHERE client_id=%s;
                """, (client_id, ))
        conn.commit()


# 7. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
                SELECT ci.first_name, ci.last_name, ci.email, p.phone_number FROM client_info ci
                LEFT JOIN phones p ON ci.client_id = p.client_id OR ci.client_id IS NULL
                WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
                AND (last_name = %(last_name)s OR %(last_name)s IS NULL)
                AND (email = %(email)s OR %(email)s IS NULL)
                AND (phone_number = %(phone_number)s or %(phone_number)s IS NULL)
                ORDER BY ci.last_name, ci.first_name
                ;
                """,
                    {
                        'first_name': first_name,
                        'last_name': last_name,
                        'email': email,
                        'phone_number': phone
                    }
                    )
        print(f'Задание 7\n'
              f'Дано: \n'
              f'first_name = {first_name}\n'
              f'last_name = {last_name}\n'
              f'email = {email}\n'
              f'phone = {phone}\n\n'
              f'Результат поиска:')
        pprint(cur.fetchall())
        print()



with psycopg2.connect(database="clients_db", user="", password="") as conn:

    # Создаем таблицы БД
    create_db(conn)

    # Добавляем клиентов
    add_client(conn, "Анна", "Иванова", "iva@ya.ru", ("79999999999", "111111111", "+22222222"))
    add_client(conn, "Петр", "Петров", "petya@gmail.com")
    add_client(conn, "Петр", "Александров", "abc@gmail.com", 123456)
    add_client(conn, "Иван", "Сидоров", "zaq@gmail.com", 12345111)
    add_client(conn, "Ирина", "Сергеева", "irina.s@mail.ru")

    # Добавляем телефонные номера
    add_phone(conn, 2, "11111")
    add_phone(conn, 3, "00000")

    # Вносим изменения в информацию о клиенте
    change_client(conn, 2, "Петр","Сидоров", "petya@gmail.com")

    # Удаляем номер телефона
    delete_phone(conn, 1, "79999999999")

    # Удаляем колиента из БД (в том числе все записи с его номерами телефонов из таблицы phones)
    delete_client(conn, 1)

    # Ищем клиентов по параметрам
    find_client(conn)
    find_client(conn, first_name="Петр", phone="00000")
    find_client(conn, first_name="Петр", last_name="Александров", phone="123456")
    find_client(conn, email="petya@gmail.com")
    find_client(conn, last_name="Сидоров")
    find_client(conn, first_name="Ирина")


conn.close()

