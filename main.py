from dadata import Dadata

token = "83ee29b164712c40a3104fb10813bde0457e23f6"
secret = "b8f8e51f7213f8f741899ad691c68fce1b16b1de"
for i in range(9000):
    ddt = Dadata(token, secret)
    result = ddt.clean("address", "карламарксадом23северодвинск")
    print(result)
