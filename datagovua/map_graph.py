from datagovua.type_filter import get_data_by_type
from datagovua.city_code import get_city_by_code


def create_map():
    dep_code = get_data_by_type("dep")
    city_by_code = get_city_by_code()

    registrations: dict = {}
    for year in dep_code:
        for reg_count in dep_code[year]:
            try:
                code = reg_count['dep'].split(" ")
                if code[0] == "ТСЦ":
                    city = city_by_code[code[1]]
                    try:
                        registrations[city] += int(reg_count['count'])
                    except KeyError:
                        registrations[city] = int(reg_count['count'])
            except KeyError:
                print("Not found city by departure code: {}".format(reg_count['dep']))

    registrations_data_set: list = []
    for city in registrations:
        if city == "м. Кривий":
            city_name = "м. Кривий Ріг"
        else:
            city_name = city
        registrations_data_set.append(['{}, Україна'.format(city_name), '{}'.format(str(registrations[city])), 'pin'])

    file = open("index.html", "r", encoding="utf-8")
    page = file.read()
    file.close()

    page = page.replace("datasets", str(registrations_data_set))

    file = open("../index.html", "w", encoding="utf-8")
    file.write(page)
    file.close()
