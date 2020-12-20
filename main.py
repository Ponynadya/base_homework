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

with closing(psycopg2.connect(dbname='json_database', user='postgres', password='qauser',
                              host='localhost')) as conn:
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute('INSERT INTO goods (name, package_height, package_width) '
                       'VALUES (%s, %s, %s);', (good['name'], good['package_height'], good['package_width']))
        cursor.execute('SELECT * FROM goods')
        data_in_goods = cursor.fetchall()

        shop_goods = []
        for data in parsed_data['location_and_quantity']:
            shop_goods.append(dict(id_good=data_in_goods[0][0],
                                   location=data['location'],
                                   amount=data['amount']))
        for shop_good in shop_goods:
            cursor.execute('insert into shop_goods (id_good, location, amount) '
                           'VALUES (%s, %s, %s);', (shop_good['id_good'], shop_good['location'], shop_good['amount']))
        conn.commit()
