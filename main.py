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
    corps_pattern = r'\bк\d{1,}'

    loc = re.findall(loc_pattern, address)
    street = re.findall(street_pattern, address)
    building = re.findall(building_pattern, address)
    corps = re.findall(corps_pattern, address)
    return loc, street, building, corps


def clear_loc(loc):
    """
    Убирает населенный пункт, оставляя только рабочую часть
    """
    loc_sub_pattern = r'\b(?:округ|окр|д|дер|деревня|г|гор|город|с|село|п|пос|поселок|проезд|рп|снт|СНТ)\b\.*\s*'

    location = re.sub(loc_sub_pattern, r'', loc[-1])
    return location


def clear_street(street):
    """
    Убирает ул улицу и т.д, оставляя только рабочую часть
    """
    street_sub_pattern = r'\b(?:ул\.\sим\.s\проф|ул\.*\s*им|улица\sим|улица|ул|у|пер\.*\s*им|пер|переулок|наб\.\s*им|наб|набережная|набер|ш|шоссе|пр\.\sим|пр\-кт|пр|проспект|кв\-л|кв|квартал)\b\.*\s*'

    street = re.sub(street_sub_pattern, r'', street[0])
    print(street, '!!!!!!!!!!!!')
    return street


def clear_building(building):
    """
    Убирает д., дом, оставляя только рабочую часть
    """
    building_sub_pattern = r'\b(?:д|дом)\b\.*\s*'

    build = re.sub(building_sub_pattern, r'', building[0])
    return build


def update_table(cn, lat, lon):
    cur2 = connection.cursor()
    cur2.execute(
        f"""
        UPDATE all_oks_b
        SET latitude = {lat}, longitude = {lon}
        WHERE cn = {"'" + cn + "'"}
        """
    )
    cur2.close()
    connection.commit()


def get_string_to_geocode(loc, street, building, corps):
    if len(street) != 0:
        if len(corps) == 0:
            geocode_string = f'{loc}, {street} {building}'
        else:
            geocode_string = f'{loc}, {street} {building} {corps[0]}'

    else:
        if len(corps) == 0:
            geocode_string = f'{loc}, {building}'
        else:
            geocode_string = f'{loc}, {building} {corps[0]}'

    return geocode_string


geolocator = Nominatim(user_agent="user_agent")

# limit = 5000
count = 0
success_count = 0

try:
    connection = psycopg2.connect(
        user="postgres",
        password="32203005",
        host="localhost",
        port="5432"
    )

    cur = connection.cursor()

    cur.execute(
        f"""
        SELECT address, cn
        FROM ar_oks
        WHERE latitude IS NULL
        """
    )

    data = True  # just to start process
    while data:
        count += 1
        print(count)

        try:
            data = cur.fetchone()
            print(data)
        except psycopg2.ProgrammingError as e:
            data = ('empty', 'empty')

        address = data[0]
        cn = data[1]
        print(f'Initial address is:\n\t{address}\n')

        loc, street, building, corps = transform_data(address)
        print(f'Initial data:\n\t{loc}, {street}, {building}, {corps}')

        if len(loc) == 0 or len(building) == 0:
            print('-----')
            print('[GEOCODING ERROR] NOT ENOUGH DATA')
            print('_______')

        else:
            if len(street) > 0:
                loc = clear_loc(loc)
                street = clear_street(street)
                building = clear_building(building)

            else:
                loc = clear_loc(loc)
                building = clear_building(building)

            geocode_string = get_string_to_geocode(loc, street, building, corps)
            print(f'Workable data:\n\t{geocode_string}')

            location = geolocator.geocode(geocode_string, timeout=None)

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

                update_table(cn, lat, lon)

                success_count += 1

    connection.commit()
    cur.close()

    print('********')
    print('INFO: [SUCCESSFULLY]')

except Exception as error:
    print('********')
    print("INFO: [Error while working with PostgreSQL]", error, type(error))

except KeyboardInterrupt:
    print('INFO: EXECUTION STOPPED')

finally:
    if connection:
        connection.close()
        print("INFO: [Connection with PostgreSQL closed]")
        print(
            f'INFO: Success issues {success_count} from {count} attempts, percentage {round((success_count / count * 100), 1)}%')

# average success 82%
