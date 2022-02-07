import psycopg2
import re
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="user_agent")

address = 'Кодино, ул. Набережная, д. 30'

location = geolocator.geocode(address)

lat = location.latitude
lon = location.longitude
print(lat, lon)

