from dadata import Dadata

token = "5cd6e640d13d4a5d8aabcc06954ba984ffad0171"
secret = "656e076f47f6982f6639a1c1000426b9bc511941"
for i in range(1):
    ddt = Dadata(token, secret)
    result = ddt.clean("address", "карла маркса дом 23 северодвинск")
    print(result)
