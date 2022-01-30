import re


def out_red(text):
    print("\033[31m {}".format(text))


def out_yellow(text):
    print("\033[33m {}".format(text))


def out_blue(text):
    print("\033[34m {}".format(text))


"""
АДРЕСА НА ПРОВЕРКУ:

Архангельская область, р-н. Няндомский, г. Няндома, ул. Кирпичная, д. 4А 

?

Архангельская область, р-н. Плесецкий, рп. Савинский, ул. 9 Мая, д. 13
Архангельская область, р-н. Котласский, г. Котлас, ул. О.Кошевого, д. 29
Архангельская область, г. Коряжма, снт. Садоводы Севера сад N4, ул. Майская, д. 78
Архангельская область, р-н. Вельский, г. Вельск, ул. К.Маркса, д. 36
Архангельская обл., Плесецкий муниципальный район, МО “Самодедское”,  п. Самодед, ул. Первомайская, д. 1б, №2
Архангельская область, р-н. Лешуконский, с. Койнас, д. 5
Российская Федерация, Архангельская область, м.о. Каргопольский, д. Меньшаковская, ул. 2-я Линия, д. 4
Архангельская область,  Вельский район, МО «Усть-Вельское», д. Ленино-Ульяновская, д. 35
Архангельская область, МО "Котлас", п. Вычегодский, ул. Ульянова, д. 51, корпус 1
Архангельская область, р-н. Виноградовский, рп. Березник, ул. Массив Придорожный-2, д. 2О
Российская Федерация, Архангельская область, городской округ Северодвинск, территория садоводческого некоммерческого товарищества Радуга, ряд 3, земельный участок 13
Архангельская область, р-н. Холмогорский, с. Ломоносово, д. 44А, корп. 1
Архангельская область, р-н. Котласский, г. Котлас, рп. Вычегодский, ул. Энгельса, д. 72, корп. 8
Архангельская обл., Приморский р-н, пос. Талаги, д. 31/20
Архангельская область, Вельский район, МО "Муравьевское", д. Горка Муравьевская, пер. Кирилловский, д. 2, здание № 12
Архангельская обл., г. Архангельск, Исакогорский район, 1-я линоя, д. 64
"""

address = 'Архангельская область, р-н. Котласский, г. Котлас, ул. О.Кошевого, д. 29'

loc_pattern = r'\b(?:д|дер|деревня|г|гор|город|с|село|п|пос|поселок|рп)\b\.*\s*[а-яА-Я]+'

street_pattern = r'\b(?:ул|улица|у|пер|переулок|наб|набережная|набер|ш|шоссе|пр|проспект|пр\-кт)\b\.*\s*[а-яА-Я]+\.*[а-яА-Я]+'

building_pattern = r'\b(?:д|дом)\b\.*\s*\d+\s*\w{0,1}'

loc = re.findall(loc_pattern, address)
# address = re.sub(loc_pattern, '', address)

street = re.findall(street_pattern, address)
# address = re.sub(street_pattern, '', address)

building = re.findall(building_pattern, address)
# address = re.sub(building_pattern, '', address)

print(f'Адрес: {address}\nЛокация: {loc}\nУлица: {street}\nЗданиие{building}')

# out_red("Вывод красным цветом")
# out_yellow("Текст жёлтого цвета")
# out_blue("Синий текст")
