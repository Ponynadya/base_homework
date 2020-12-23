import json
import jsonschema
import psycopg2
from psycopg2.extras import DictCursor
from contextlib import closing

file = 'data.json'
schema = 'goods.schema.json'

with open(file, 'r') as f:
    parsed_data = json.load(f)

with open(schema, 'r') as f:
    schema_data = json.load(f)

result = jsonschema.validate(parsed_data, schema_data)

good = dict(name=parsed_data['name'],
            package_height=parsed_data['package_params']['height'],
            package_width=parsed_data['package_params']['width'])

with closing(psycopg2.connect(dbname='postgres', user='postgres', password='abc123',
                              host='localhost')) as conn:
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute('CREATE TABLE if not exists goods (name VARCHAR(255) UNIQUE, package_height INTEGER, '
                       'package_width INTEGER, id SERIAL, PRIMARY KEY (id)) ')
        cursor.execute('INSERT INTO goods (name, package_height, package_width) VALUES ( %s, %s, %s) '
                       'ON CONFLICT (name) DO UPDATE SET package_height = EXCLUDED.package_height, '
                       'package_width = EXCLUDED.package_width',
                       (good['name'], good['package_height'], good['package_width']))
        cursor.execute('SELECT * FROM goods')
        data_in_goods = cursor.fetchall()
        conn.commit()
        shop_goods = []
        for data in parsed_data['location_and_quantity']:
            shop_goods.append(dict(id_good=data_in_goods[0][-1],
                                   location=data['location'],
                                   amount=data['amount']))

        cursor.execute('CREATE TABLE if not exists shop_goods (id_good INTEGER,'
                       ' location VARCHAR(255),'
                       ' amount VARCHAR(255), UNIQUE (id_good, location))')
        conn.commit()

        for shop_good in shop_goods:
            cursor.execute('INSERT INTO shop_goods (id_good, location, amount) '
                           'VALUES (%s, %s, %s) ON CONFLICT (id_good, location)'
                           ' DO UPDATE SET amount = EXCLUDED.amount',
                           (shop_good['id_good'], shop_good['location'], shop_good['amount']))
        conn.commit()
