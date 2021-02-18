from time import time
from functools import reduce
from multiprocessing import Pool


mapper = len


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
    print("Reading file...")
    start = time()
    file = open("tz_opendata_z01012017_po31122017.csv", "r", encoding="utf-8")
    file.readline()
    rows = file.readlines()
    file.close()

    open_data = []
    for row in rows:
        row = row.replace("\n", "")
        columns = row.split(";")
        for column in columns:
            open_data.append(column)
    finish = time()
    read_time = finish - start
    print("Finish read data...\nTime: {}\n".format(read_time))

    data_chunks = chunkify(open_data, 8)

    print("Start mapreduce...")
    start = time()
    with Pool(8) as pool:
        mapped = pool.map(chunks_mapper, data_chunks)
        reduced = reduce(reducer, mapped)
        print("Result:\n{}".format(reduced))
    finish = time()
    mapreduce_time = finish - start
    print("Finish mapreduce.\nTime: {}".format(mapreduce_time))
    print("Total time: {}".format(read_time + mapreduce_time))
