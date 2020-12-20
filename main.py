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
        cursor.execute(f'INSERT INTO goods (name, package_height, package_width) '
                       f'VALUES ("{good["name"]}", {good["package_height"]}, {good["package_width"]});')


# shop_goods = []
#
# for good_in_shop in result['location_and_quantity']:
#     shop_goods.append(dict(id_good=))

# take from DB
# take ID



