from time import time
from functools import reduce
from multiprocessing import Pool

mapper = len
list_of_strings = ['abc', 'python', 'dima']
large_list_of_strings = list_of_strings*10000000


def reducer(p, c):
    if p[1] > c[1]:
        return p
    return c


def chunkify(seq, size):
    return (seq[i::size] for i in range(size))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)
    mapped_chunk = zip(chunk, mapped_chunk)
    return reduce(reducer, mapped_chunk)


if __name__ == '__main__':
    data_chunks = chunkify(large_list_of_strings, 8)
    start = time()
    with Pool(8) as pool:
        mapped = pool.map(chunks_mapper, data_chunks)
        reduced = reduce(reducer, mapped)
        print(reduced)
    finish = time()
    time_result = finish - start
    print("Time {}".format(time_result))
