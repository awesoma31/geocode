import psycopg2
import re
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="user_agent")

try:
    connection = psycopg2.connect(
        user="postgres",
        password="32203005",
        host="localhost",
        port="5432"
    )
    # connection.autocommit = True

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT address
            FROM all_oks_b_h
            ORDER BY RANDOM()
            LIMIT 10000
            """
        )

        loc_pattern = r'\b(?:д|дер|деревня|г|гор|город|с|село|п|пос|поселок|рп|ж/д_ст)\b\.*\s+[а-яА-Я\-]+'
        street_pattern = r'\b(?:ул|улица|у|пер|переулок|наб|набережная|набер|ш|шоссе|пр|проспект|пр\-кт)\b\.*\s*[а-яА-Я]+\.*[а-яА-Я]+'
        building_pattern = r'\b(?:д|дом)\b\.*\s*\d+\s*\w{0,1}'

        count = 1
        for i in range(1000):
            print(count)
            count += 1
            address = cur.fetchone()
            address = address[0]
            print(address)
            loc = re.findall(loc_pattern, address)
            street = re.findall(street_pattern, address)
            building = re.findall(building_pattern, address)
            print(loc, street, building)

        # location = geolocator.geocode('Березник, ул. П.Виноградова, д. 172')
        # print((location.latitude, location.longitude))
        # print(location.raw)

        connection.commit()
        print()
        print('INFO: [SUCCESSFULLY]')

except Exception as error:
    print()
    print("INFO: [Ошибка при работе с PostgreSQL]", error)
finally:
    if connection:
        connection.close()
        print("INFO: [Соединение с PostgreSQL закрыто]")

"""
SELECT address
FROM all_oks_b_h
WHERE address SIMILAR TO '%(д|дер|г|гор|с.п|п|с|р.п|город).?\s[а-яА-Я]+%(ул|улица|пер|п|пр|наб).?\s*[а-яА-Я]+%(д|дом).?\d%';
"""
