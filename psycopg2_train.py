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
            """
        )
        pattern = r'\b(?:д|дер|деревня|г|гор|город|c|село|п|пос|поселок)\b\.*\s+[а-яА-Я]+'
        count = 1

        for i in range(10000):
            print(count)
            count += 1
            loc = cur.fetchone()
            loc = loc[0]
            print(loc)
            loc = re.findall(pattern, loc)
            if len(loc) != 0:
                loc = loc[0]
                print(loc)

        # location = geolocator.geocode(rec[0])
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
