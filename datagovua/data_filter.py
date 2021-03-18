"""
Lambda function for get personal info about teacher car.

This function use unless modules:

The function can not be a module but can be used as an API.
"""
from time import time
from matplotlib.pyplot import savefig
from datagovua.type_filter import get_limit_data_by_type
from datagovua.reg_by_year import get_data_by_year
from datagovua.map_graph import create_map
from datagovua.plot_maker import bar_builer, compress_dict, pie_builer


if __name__ == '__main__':
    start = time()

    create_map()
    brand = get_limit_data_by_type('brand')
    color = get_limit_data_by_type('color')
    make_year = get_limit_data_by_type('make_year')
    reg_year = get_data_by_year()
    tmp = {}
    for year in reg_year:
        tmp[year] = reg_year[year][1]['count']

    compress_dict(brand)
    for year in brand:
        fig, axes = bar_builer(brand[year], year)
        savefig("../img/{}.png".format("brand" + str(year)))

    compress_dict(color)
    for year in color:
        pie_builer(color[year], year)
        savefig("../img/{}.png".format("color" + str(year)))

    compress_dict(make_year)
    for year in make_year:
        bar_builer(make_year[year], year)
        savefig("../img/{}.png".format("make_year" + str(year)))

    bar_builer(tmp, "2021")
    savefig("../img/reg_year.png")

    end = time()
    print("Work time: {:0.3f}".format(end - start))
