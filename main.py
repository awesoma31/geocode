import psycopg2
import re
from geopy.geocoders import Nominatim


def transform_data(address):
    """
    Transform Initial data into Workable data
    """
    loc_pattern = r'\b(?:округ|окр|д|дер|деревня|г|гор|город|с|село|п|пос|поселок|проезд|рп|снт|СНТ)\b\.*\s*[а-яА-Я\-]+\d*\s*[а-яА-Я\-]*'
    street_pattern = r'\b(?:ул\.\sим\.s\проф|ул\.*\s*им|улица\sим|улица|ул|у|пер\.*\s*им|пер|переулок|наб\.\s*им|наб|набережная|набер|ш|шоссе|пр\.\sим|пр\-кт|пр|проспект|кв\-л|кв|квартал)\b\.*\s*[а-яА-Я\s\-]+\d*\.*\s*[а-яА-Я\-]+\d*\.*\s*[а-яА-Я\-]+\d*'
    building_pattern = r'\b(?:д|дом)\b\.*\s*[\d]+\s*\w{0,1}\b'

    loc = re.findall(loc_pattern, address)
    street = re.findall(street_pattern, address)
    building = re.findall(building_pattern, address)
    return loc, street, building


def clear_loc(loc):
    """
    Убирает ул., улица и т.д., оставляя только рабочую часть
    """
    loc_sub_pattern = r'\b(?:округ|окр|д|дер|деревня|г|гор|город|с|село|п|пос|поселок|проезд|рп|снт|СНТ)\b\.*\s*'

    location = re.sub(loc_sub_pattern, r'', loc[-1])
    return location


def clear_building(building):
    """
    Убирает д., дом, оставляя только рабочую часть
    """
    building_sub_pattern = r'\b(?:д|дом)\b\.*\s*'

    build = re.sub(building_sub_pattern, r'', building[0])
    return build


geolocator = Nominatim(user_agent="user_agent")

limit = 10

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
            SELECT address, cn
            FROM all_oks_b
            ORDER BY RANDOM()
            LIMIT {limit}
            """
        )

        # check_random_addresses()

        count_1 = 0
        success_count = 0

        for i in range(limit):
            count_1 += 1
            print(count_1)

            data = cur.fetchone()
            address = data[0]
            cn = data[1]
            print(f'Initial address is:\n\t{address}\n')
            # print(cn, type(cn))

            loc, street, building = transform_data(address)
            print(f'Initial data:\n\t{loc}, {street}, {building}')

            if len(loc) <= 0 or len(building) <= 0:
                print('-----')
                print('[GEOCODING ERROR] NOT ENOUGH DATA')
                print('_______')

            else:
                if len(street) > 0:
                    loc = clear_loc(loc)
                    building = clear_building(building)
                    print(f'Workable data:\n\t{loc}, {street[0]} {building}\n')

                    location = geolocator.geocode(f'{loc}, {street[0]} {building}')

                    if location is None:
                        print('[GEOCODING ERROR] Location type is None')
                        print('_______')
                    else:
                        lat = location.latitude
                        lon = location.longitude
                        print(lat, lon)
                        print('-----')
                        print('[GEOCODING SUCCESS]')
                        print('_______')

                        with connection.cursor() as cur:
                            cur.execute(
                                f"""
                                UPDATE all_oks_b
                                SET latitude = {lat}, longitude = {lon}
                                WHERE cn = {"'" + cn + "'"}
                                """
                            )
                            connection.commit()
                        success_count += 1

                else:
                    loc = clear_loc(loc)
                    clear_building(building)
                    print(f'Workable data:\n\t{loc}, {building}\n')

                    location = geolocator.geocode(f'{loc} {building}')

                    if location is None:
                        print(f'[GEOCODING ERROR] Location type is None')
                        print('_______')

                    else:
                        lat = location.latitude
                        lon = location.longitude
                        print(lat, lon)
                        print('-----')
                        print('[GEOCODING SUCCESS]')
                        print('_______')

                        with connection.cursor() as cur:
                            cur.execute(
                                f"""
                                UPDATE all_oks_b
                                SET latitude = {lat}, longitude = {lon}
                                WHERE cn = {"'" + cn + "'"}
                                """
                            )
                            connection.commit()
                        success_count += 1

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
        print(f'INFO: Success issues {success_count}')
