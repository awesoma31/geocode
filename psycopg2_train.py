import psycopg2
import re
from geopy.geocoders import Nominatim


def check_random_addresses():
    global loc_pattern, street_pattern, building_pattern
    count = 1
    for i in range(100):
        print(count)
        count += 1
        r_address = (cur.fetchone())[0]
        print(r_address)

        r_loc = re.findall(loc_pattern, r_address)
        r_street = re.findall(street_pattern, r_address)
        r_building = re.findall(building_pattern, r_address)
        print(r_loc, r_street, r_building)


geolocator = Nominatim(user_agent="user_agent")
limit = 5

loc_pattern = r'\b(?:округ|окр|д|дер|деревня|г|гор|город|с|село|п|пос|поселок|проезд|рп|снт|СНТ)\b\.*\s*[а-яА-Я\-]+\d*\s*[а-яА-Я\-]*'
street_pattern = r'\b(?:ул\.\sим\.s\проф|ул\.*\s*им|улица\sим|улица|ул|у|пер\.*\s*им|пер|переулок|наб\.\s*им|наб|набережная|набер|ш|шоссе|пр\.\sим|пр\-кт|пр|проспект|кв\-л|кв|квартал)\b\.*\s*[а-яА-Я\s\-]+\d*\.*\s*[а-яА-Я\-]+\d*\.*\s*[а-яА-Я\-]+\d*'
building_pattern = r'\b(?:д|дом)\b\.*\s*[\d]+\s*\w{0,1}\b'

loc_sub_pattern = r'\b(?:округ|окр|д|дер|деревня|г|гор|город|с|село|п|пос|поселок|проезд|рп|снт|СНТ)\b\.*\s*'
building_sub_pattern = r'\b(?:д|дом)\b\.*\s*'

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
            f"""
            SELECT address
            FROM all_oks_b_h
            ORDER BY RANDOM()
            LIMIT {limit}
            """
        )

        # check_random_addresses()

        count_1 = 0
        for i in range(limit):
            count_1 += 1
            print(count_1)

            address = cur.fetchone()
            address = address[0]
            print(f'Initial address is:\n\t{address}\n')

            loc = re.findall(loc_pattern, address)
            street = re.findall(street_pattern, address)
            building = re.findall(building_pattern, address)
            print(f'Initial data:\n\t{loc}, {street}, {building}')

            if len(loc) > 0 and len(building) > 0:
                if len(street) > 0:
                    loc = re.sub(loc_sub_pattern, r'', loc[-1])
                    building = re.sub(building_sub_pattern, r'', building[0])
                    print(f'Workable data:\n\t{loc}, {street[0]} {building}\n')

                    # location = geolocator.geocode(f'{loc}, {street[-1]} {building[-1]}')
                    location = geolocator.geocode(f'{loc}, {street[0]} {building}')

                    if location is None:
                        print('[GEOCODING ERROR] Location type is None')
                        print('_______')
                    else:
                        lat = location.latitude
                        lon = location.longitude
                        print((lat, lon))
                        print('-----')
                        print('SUCCESS')
                        print('_______')
                else:
                    loc = re.sub(loc_sub_pattern, r'', loc[-1])
                    building = re.sub(building_sub_pattern, r'', building[0])
                    print(f'Workable data:\n{loc}, {building}\n')

                    # location = geolocator.geocode(f'{loc}, {street[-1]} {building[-1]}')
                    location = geolocator.geocode(f'{loc} {building}')

                    if location is None:
                        print(f'[GEOCODING ERROR] Location type is None')
                        print('_______')
                    else:
                        lat = location.latitude
                        lon = location.longitude
                        print(lat, lon)
                        print('-----')
                        print('SUCCESS')
                        print('_______')
                    # print(location.raw)

        connection.commit()
        print('********')
        print('INFO: [SUCCESSFULLY]')

except Exception as error:
    print('********')
    print("INFO: [Ошибка при работе с PostgreSQL]", error)
finally:
    if connection:
        connection.close()
        print("INFO: [Соединение с PostgreSQL закрыто]")
