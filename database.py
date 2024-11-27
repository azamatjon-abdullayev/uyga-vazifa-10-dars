import psycopg2
from django.db.transaction import commit


class DataBase:
    def __init__(self):
        self.database = psycopg2.connect(
            database='fn27',
            user='postgres',
            host='localhost',
            password='1'
        )

    def manager(self, sql, *args, commit=False, fetchone=False, fetchall=False):
        with self.database as db:
            with db.cursor() as cursor:
                cursor.execute(sql, args)
                if commit:
                    result = db.commit()
                elif fetchone:
                    result = cursor.fetchone()
                elif fetchall:
                    result = cursor.fetchall()
            return result

    def create_tables(self):
        sql = '''CREATE TABLE IF NOT EXISTS brands(
	            brand_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	            brand_name VARCHAR(20) not null unique
                );
                CREATE TABLE IF NOT EXISTS colors(
	            color_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	            color_name VARCHAR(20) not null
                );
                CREATE TABLE IF NOT EXISTS models(
	            model_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	            model_name VARCHAR(30) not null,
	            model_price INTEGER,
	            brand_id INTEGER REFERENCES brands(brand_id),
	            color_id INTEGER REFERENCES colors(color_id),
	            car_count INTEGER
                );
                CREATE TABLE IF NOT EXISTS customers(
	            customer_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	            first_name VARCHAR(30),
	            last_name VARCHAR(30),
	            birth_date DATE,
	            phone_number VARCHAR(15),
	            email VARCHAR(30),
	            country VARCHAR(30),
	            city VARCHAR(30)
                );
                CREATE TABLE IF NOT EXISTS employees(
	            employee_id INTEGER PRIMARY KEY,
	            first_name VARCHAR(30),
	            last_name VARCHAR(30),
	            birth_date DATE,
	            phone_number VARCHAR(15),
	            email VARCHAR(30),
	            country VARCHAR(30),
	            city VARCHAR(30)
                );

                CREATE TABLE IF NOT EXISTS orders(
	            order_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	            customer_id INTEGER REFERENCES customers(customer_id),
	            employee_id INTEGER REFERENCES employees(employee_id),
	            model_id INTEGER REFERENCES models(model_id),
	            car_count INTEGER,
	            order_date DATE
                );'''
        self.manager(sql,commit=True)

    def insert_brand(self,brand_name):
        sql = '''insert into brands(brand_name) values(%s)
        on conflict do nothing'''
        self.manager(sql,brand_name,commit=True)

    def drop_tables(self):
        sql = '''
        drop table if exists orders;
        drop table if exists employees;
        drop table if exists customers;
        drop table if exists models;
        drop table if exists colors;
        drop table if exists brands;'''
        self.manager(sql,commit=True)

    def insert_data(self):
        sql = '''
INSERT INTO brands(brand_name) VALUES ('Malibu');
INSERT INTO brands(brand_name) VALUES ('Lamborghini');
INSERT INTO brands(brand_name) VALUES ('BMW');
INSERT INTO brands(brand_name) VALUES ('Cobalt');
INSERT INTO brands(brand_name) VALUES ('Nexia');
INSERT INTO brands(brand_name) VALUES ('Rolls-Royce');

INSERT INTO colors(color_name) VALUES ('White');
INSERT INTO colors(color_name) VALUES ('Black');
INSERT INTO colors(color_name) VALUES ('Green');
INSERT INTO colors(color_name) VALUES ('Yello');
INSERT INTO colors(color_name) VALUES ('Purple');
INSERT INTO colors(color_name) VALUES ('Pretty');
INSERT INTO colors(color_name) VALUES ('Red');
INSERT INTO colors(color_name) VALUES ('Magenta');

INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Chevrolet Malibu 1', 12000, 1, 2, 5);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Chevrolet Malibu 2', 12000, 1, 6, 10);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Lamborghini Huracan', 500000, 2, 4, 4);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Lamborghini Aventador', 700000, 2, 5, 3);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Lamborghini Urus', 600000, 2, 4, 5);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Lamborghini Revuelto', 200000, 2, 8, 2);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Lamborghini Gallardo', 300000, 2, 7, 7);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW XM', 250000, 3, 5, 6);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW X7 M60i xDrive', 70000, 3, 6, 5);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW X7 M50d', 30000, 3, 1, 4);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW X6 M', 100000, 3, 3, 7);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW X6 M Competition', 110000, 3, 4, 9);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW X5 M', 50000, 3, 4, 8);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('BMW М8 Gran Coupe', 75000, 3, 6, 2);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Chevrolet Cobalt', 12000, 4, 5, 10);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Chevrolet Cobalt 2', 13000, 4, 7, 9);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Nexia 1', 5000, 5, 2, 18);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Nexia 2', 10000, 5, 4, 12);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Nexia 3', 12000, 5, 3, 17);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Phantom', 75000, 6, 7, 5);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Specte', 87000, 6, 6, 8);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Cullinan', 92000, 6, 5, 7);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Ghost', 67000, 6, 4, 9);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Dawn', 22000, 6, 1, 3);
INSERT INTO models(model_name, model_price, brand_id, color_id, car_count) VALUES('Rolls-Royce Wraith', 40000, 6, 3, 6);

INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Sobir', 'Jalilov', '1990-06-19', '+998991234567', 'sobir@gmail.com', 'Uzbekistan', 'Fegana');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Toxir', 'Toxirov', '1991-06-20', '+998991234578', 'toxir@gmail.com', 'Qozoqiston', 'Ostona');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Kim', 'Min', '2000-11-22', '+9989912345555', 'kim@gmail.com', 'Korea');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Botir', 'Sobirov', '1990-07-20', '+9989912345566', 'botir@gmail.com', 'Uzbakistan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Gulnoza', 'Zokirova', '1990-06-22', '+9989912355555', 'gulnoza@gmail.com', 'Tojikiston');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Maqsud', 'Ibrohiov', '1990-06-23', '+9989912377555', 'maqsud@gmail.com', 'Uzbekistan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Liam', 'Noah', '1990-06-24', '+9989912755555', 'liam@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Oliver', 'James', '1990-06-18', '+9989912346655', 'oliver@gmail.com', 'Great Britan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Komila', 'Jalilova', '1990-06-17', '+9989912345566', 'komila@gmail.com', 'Qozoqiston');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Alisher', 'Maqsudov', '1990-06-01', '+9989912345577', 'alisher@gmail.com', 'Qirgzistan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Hasan', 'Husanov', '1990-06-02', '+998991234588', 'hasan@gmail.com', 'Uzbekistan', 'Tashkent');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Komil', 'Zokirov', '1990-06-06', '+9989912345599', 'komil@gmail.com', 'Qozoqiston');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Hayot', 'Bakirov', '1990-06-03', '+998991234510', 'hayot@gmail.com', 'Tojikiston', 'Dushanbe');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Sobir', 'Shukurov', '1990-06-07', '+998991234511', 'sobirshukurov@gmail.com', 'Uzbekistan', 'Samarqand');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Husan', 'Botirov', '1990-06-16', '+9989912345512', 'husan@gmail.com', 'Qirgzistan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Shukur', 'Bakirov', '1990-06-15', '+998991234513', 'shukur@gmail.com', 'Uzbekistan', 'Fergana');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Mixail', 'Jon', '1990-06-08', '+9989912345514', 'mixail@gmail.com', 'Mexiko');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Aldo', 'Nik', '1990-06-20', '+9989912345515', 'aldo@gmail.com', 'Italia');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Elijah', 'James', '1990-06-21', '+9989912345516', 'elijah@gmail.com', 'Italia');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Wilyam', 'Henry', '1990-06-14', '+9989912345517', 'wiliam@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Kwan', 'Jo', '1990-06-11', '+998991234518', 'kwan@gmail.com', 'Korea', 'Seul');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Levi', 'Daniel', '1990-06-10', '+9989912345519', 'levi@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Owen', 'Asher', '1990-06-12', '+9989912345520', 'owen@gmail.com', 'Great Britan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Area', 'Hana', '1990-06-03', '+9989912345521', 'area@gmail.com', 'Great Britan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Ethan', 'Leo', '1990-06-02', '+9989912345522', 'etham@gmail.com', 'Italia');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Ezra', 'John', '1990-06-05', '+9989912345523', 'ezra@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Luca', 'Aiden', '1990-06-07', '+9989912345524', 'luca@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Seo', 'Joon', '1990-06-08', '+998991234526', 'seo@gmail.com', 'Korea', 'Seul');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country, city) VALUES ('Min', 'Ji', '1990-06-13', '+998991234527', 'minji@gmail.com', 'Japan', 'Tokio');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Yu', 'Jin', '1990-06-18', '+9989912345528', 'yu@gmail.com', 'China');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Isac', 'Isac', '1990-06-19', '+9989912345529', 'sac@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Tomas', 'Jayden', '1990-06-20', '+9989912345530', 'tomas@gmail.com', 'Amerika');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Min', 'Ho', '1990-06-23', '+9989912345531', 'minho@gmail.com', 'Korea');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Charles', 'Cooper', '1990-06-25', '+9989912345532', 'charles@gmail.com', 'Great Britan');
INSERT INTO customers(first_name, last_name, birth_date, phone_number, email, country) VALUES ('Ogan', 'Luca', '1990-06-27', '+9989912345533', 'organ@gmail.com', 'Great Britan');

INSERT INTO employees(employee_id, first_name, last_name, birth_date, phone_number, email, country, city) VALUES (2425, 'Nosir', 'Jalilov', '1995-05-19', '+998991232328', 'nosir@gmail.com', 'Uzbekistan', 'Fegana');
INSERT INTO employees(employee_id, first_name, last_name, birth_date, phone_number, email, country, city) VALUES (2426, 'Jalil', 'Nosirov', '1997-02-28', '+998991233338', 'jaliln@gmail.com', 'Uzbekistan', 'Andijon');
INSERT INTO employees(employee_id, first_name, last_name, birth_date, phone_number, email, country, city) VALUES (2727, 'Anvar', 'Bakirov', '1993-06-15', '+998991232429', 'anvar@gmail.com', 'Uzbekistan', 'Fegana');
INSERT INTO employees(employee_id, first_name, last_name, birth_date, phone_number, email, country, city) VALUES (2428, 'Bakir', 'Anvarov', '1994-05-23', '+998991232530', 'bakira@gmail.com', 'Uzbekistan', 'Fegana');
INSERT INTO employees(employee_id, first_name, last_name, birth_date, phone_number, email, country, city) VALUES (2429, 'Odil', 'Javlonov', '1999-06-13', '+998991232637', 'odil@gmail.com', 'Uzbekistan', 'Namangan');

INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (1, 2425, 4, 2, '2016-09-05');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (2, 2425, 12, 1, '2016-09-05');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (3, 2727, 5, 1, '2016-09-05');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (4, 2428, 22, 1, '2016-09-06');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (2, 2426, 18, 1, '2016-09-06');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (5, 2428, 11, 1, '2016-09-06');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (6, 2429, 19, 1, '2016-09-06');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (7, 2426, 25, 1, '2016-09-07');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (8, 2727, 8, 1, '2016-09-07');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (9, 2425, 7, 2, '2016-09-08');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (10, 2425, 9, 1, '2016-09-08');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (11, 2429, 12, 1, '2016-09-08');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (5, 2426, 1, 1, '2016-09-08');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (12, 2428, 6, 1, '2016-09-09');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (13, 2429, 23, 1, '2016-09-10');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (14, 2428, 19, 1, '2016-09-10');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (15, 2425, 24, 1, '2016-09-11');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (10, 2425, 11, 1, '2016-09-11');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (16, 2727, 15, 3, '2016-09-11');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (17, 2727, 1, 1, '2016-09-12');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (18, 2425, 17, 1, '2016-09-12');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (19, 2426, 8, 1, '2016-09-12');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (16, 2429, 7, 1, '2016-09-12');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (20, 2428, 13, 1, '2016-09-13');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (21, 2428, 18, 1, '2016-09-14');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (22, 2426, 24, 1, '2016-09-15');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (23, 2425, 22, 1, '2016-09-15');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (24, 2429, 4, 1, '2016-09-15');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (25, 2425, 11, 1, '2016-09-16');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (26, 2425, 5, 1, '2016-09-16');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (20, 2426, 9, 1, '2016-09-17');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (27, 2428, 10, 1, '2016-09-17');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (22, 2425, 2, 1, '2016-09-18');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (28, 2425, 17, 1, '2016-09-18');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (4, 2428, 19, 1, '2016-09-18');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (29, 2429, 25, 2, '2016-09-19');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (30, 2425, 13, 1, '2016-09-19');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (8, 2425, 16, 1, '2016-09-20');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (30, 2428, 12, 2, '2016-09-21');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (31, 2426, 6, 1, '2016-09-21');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (32, 2429, 4, 1, '2016-09-22');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (33, 2727, 21, 1, '2016-09-23');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (22, 2727, 14, 1, '2016-09-24');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (34, 2425, 7, 1, '2016-09-24');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (35, 2425, 12, 1, '2016-09-24');
INSERT INTO orders(customer_id, employee_id, model_id, car_count, order_date) VALUES (9, 2727, 22, 1, '2016-09-25');
'''
        self.manager(sql,commit=True)

    def select_brands(self):
        sql = '''select * from brands;'''
        return self.manager(sql,fetchall=True)

    def select_models(self):
        sql = '''select * from models;'''
        self.manager(sql,fetchall=True)
    def select_admins(self):
        sql = '''select * from admins;'''
        self.manager(sql,fetchall=True)

    def emails(self):
        sql = '''SELECT email
                FROM employees
                UNION
                SELECT email
                FROM customers
                ORDER BY email;
                '''
        self.manager(sql,commit=True)

    def country_count2(self):
        sql = '''SELECT country, COUNT(*) AS customer_count
FROM customers
GROUP BY country
ORDER BY customer_count DESC;'''
        self.manager(sql,commit=True)

    def employees_count_from_country(self):
        sql = '''SELECT country, COUNT(*) AS employee_count
FROM employees
GROUP BY country
ORDER BY employee_count ;'''
        self.manager(sql,commit=True)

    def count_models(self):
        sql = '''SELECT b.brand_name, COUNT(m.model_id) AS model_count
FROM brands b
JOIN models m ON b.brand_id = m.brand_id
GROUP BY b.brand_name
ORDER BY model_count DESC;'''
        self.manager(sql,commit=True)
    def all_customers(self):
        sql = '''
SELECT o.order_id, 
       c.first_name , c.email AS customer_email, 
       e.first_name , e.email AS employee_email, 
       m.model_name, m.brand_id, 
       o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN employees e ON o.employee_id = e.employee_id
JOIN models m ON o.model_id = m.model_id
ORDER BY o.order_date DESC;'''
        self.manager(sql,commit=True)
    def total_sum(self):
        sql = '''SELECT SUM(model_price) AS total_price
FROM models;
'''
        self.manager(sql,commit=True)

    def total_brands(self):
        sql = '''SELECT COUNT(*) AS total_brands
FROM brands;'''
        self.manager(sql,commit=True)






db = DataBase()
