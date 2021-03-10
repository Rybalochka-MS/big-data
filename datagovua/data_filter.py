"""
Lambda function for get personal info about teacher car.

This function use unless modules:

The function can not be a module but can be used as an API.
"""
from time import time
from datagovua.type_filter import get_data_by_type
from datagovua.reg_by_year import get_data_by_year


start = time()
brand = get_data_by_type('brand')
color = get_data_by_type('color')
make_year = get_data_by_type('make_year')
reg_year = get_data_by_year()
end = time()
print("Work time: {:0.3f}".format(end-start))
