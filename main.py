import psycopg2
from pprint import pprint


# Удаление таблиц перед запуском
def delete_db(curs):
    pass

# Функция, создающая структуру БД (таблицы).
def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(15),
        last_name VARCHAR(20),
        email VARCHAR(100)
        );   
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenum(
        number VARCHAR (15) PRIMARY KEY,
        client_id INTEGER REFERENCES clientS(id)
        );
    """)
    return

# Функция, позволяющая добавить нового клиента.
def insert_client(cur, name=None, surname=None, email=None, tel=None):
    cur.execute("""
        INSERT INTO clients(name, last_name, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)

    id = cur.fetchone()[0]
    if tel is None:
        return id
    else:
        insert_tel(cur, id, tel)
        return id

# Функция, позволяющая добавить телефон для существующего клиента.
def insert_tel(cur, client_id, tel):
    cur.execute("""
        INSERT INTO phonenum(number, client_id)
        VALUES (%s, %s)
        """, (tel, client_id))
    return client_id

# Функция, позволяющая изменить данные о клиенте.
def update_client(cur, id, name=None, surname=None, email=None):
    cur.execute("""
        SELECT * from clients
        WHERE id = %s
        """, (id,))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET name = %s, last_name = %s, email =%s 
        where id = %s
        """, (name, surname, email, id))
    return id

# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM phonenum
        WHERE number = %s
        """, (number,))
    return number

# Функция, позволяющая удалить существующего клиента.
def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenum
        WHERE client_id = %s
        """, (id,))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(cur, name=None, surname=None, email=None, tel=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if tel is None:
        cur.execute("""
            SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenum p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s
            """, (name, surname, email))
    else:
        cur.execute("""
            SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenum p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, surname, email, tel))
    return cur.fetchall()


with psycopg2.connect(database="Pro_DZ-BD", user="postgres", password="postgres") as conn:
    with conn.cursor() as curs:
        # Cоздание таблиц
        create_db(curs)
        print("БД создана")
        # Добавляем 5 клиентов
        print("Добавлен клиент id: ",
              insert_client(curs, "Александр", "Тапочкин", "tapok@mail.ru"))
        print("Добавлен клиент id: ",
              insert_client(curs, "Борис", "Ковтунов",
                            "kofta@gmail.com", '79683353535'))
        print("Добавлен клиент id: ",
              insert_client(curs, "Святослав", "Штаников",
                            "shtanci@rambler.ru", '79632252525'))
        print("Добавлен клиент id: ",
              insert_client(curs, "Анна", "Трусикова",
                            "trusik@mail.ru", '74959966336'))
        print("Добавлена клиент id: ",
              insert_client(curs, "Светлана", "Пуховикова",
                            "puh@yandex.ru"))
        print("Данные в таблицах")

        curs.execute("""
            SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenum p ON c.id = p.client_id
            ORDER by c.id
            """)
        pprint(curs.fetchall())

        # Добавляем клиенту номер телефона(одному первый, одному второй)
        print("Телефон добавлен клиенту: ",
              insert_tel(curs, 2, '74995554466'))
        print("Телефон добавлен клиенту: ",
              insert_tel(curs, 1, '781233322211'))

        print("Данные в таблицах")
        curs.execute("""
            SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenum p ON c.id = p.client_id
            ORDER by c.id
            """)
        pprint(curs.fetchall())

        # Изменим данные клиента
        print("Изменены данные клиента id: ",
              update_client(curs, 4, "Оля", None, 'olya@mail.ru'))

        # Удаляем клиенту номер телефона
        print("Телефон удалён c номером: ",
              delete_phone(curs, '781233322211'))
        print("Данные в таблицах")
        curs.execute("""
            SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenum p ON c.id = p.client_id
            ORDER by c.id
            """)
        pprint(curs.fetchall())

        # Удалим клиента номер 2
        print("Клиент удалён с id: ",
              delete_client(curs, 2))
        curs.execute("""
                        SELECT c.id, c.name, c.last_name, c.email, p.number FROM clients c
                        LEFT JOIN phonenum p ON c.id = p.client_id
                        ORDER by c.id
                        """)
        pprint(curs.fetchall())
        # 7. Найдём клиента
        print('Имя клиента:')
        pprint(find_client(curs, 'Светлана'))

        print('Email клиента:')
        pprint(find_client(curs, None, None, 'olya@mail.ru'))

        print('Клиент по имени, фамилии и email:')
        pprint(find_client(curs, 'Светлана', 'Пуховикова',
                           'puh@yandex.ru'))

        print('Клиент по имени, фамилии, телефону и email:')
        pprint(find_client(curs, "Борис", "Ковтунов",
                           "kofta@gmail.com", '79683353535'))

        print('Клиент по имени, фамилии, телефону:')
        pprint(find_client(curs, None, None, None, '749599663'))
conn.close()