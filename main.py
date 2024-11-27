from click import command
from database import db
from collections import namedtuple

Brand = namedtuple('Brand',['brand_id','brand_name'])

def show_brands():
    brands = db.select_brands()
    for brand in brands:
        brand = Brand(*brand)
        print('-' * 62)
        print('|', str(brand.brand_id).center(5, ' '),'|',str(brand.brand_name).center(506,' '), '|')
        print('-' * 62)
show_brands()

def run():
    while True:

        command = input("Buyruq: ").lower()

        if command == 'stop':
            break
        elif command == 'add brand':
            brand_name = input("brand nomini kiriting: ")
            db.insert_brand(brand_name)
            print(f"{brand_name.title()} brandi saqlandi")



if __name__ == '__main__':
    # db.drop_tables().
    db.select_brands()
    db.create_tables()
    run()