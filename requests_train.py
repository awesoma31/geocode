from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="user_agent")
location = geolocator.geocode("першинская д27")
print(location.address)
print((location.latitude, location.longitude))
# print(location.raw)
