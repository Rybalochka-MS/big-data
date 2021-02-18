"""
Realize mapreduce for data.gov.ua

Used libraries:
    * time - for realize timer
    * functools - for use reduce
    * multiprocessing - realize multiprocess counting
    * collections - for use Counter

File can not use asa module.
"""
from time import time
from functools import reduce
from multiprocessing import Pool
from collections import Counter


def mapper(text):
    tokens_in_text = text.split()
    return Counter(tokens_in_text)


def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1


def chunkify(seq, size):
    return (seq[i::size] for i in range(size))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)


if __name__ == '__main__':
    # Start process
    print("Reading file...")
    start = time()
    file = open("tz_opendata_z01012017_po31122017.csv", "r", encoding="utf-8")
    # Skip header line
    file.readline()
    rows = file.readlines()
    file.close()

    # Adding all cells in one array
    open_data = []
    for row in rows:
        row = row.replace("\n", "")
        columns = row.split(";")
        for column in columns:
            open_data.append(column)
    finish = time()
    read_time = finish - start
    print("Finish read data...\nTime: {}\n".format(read_time))

    # Split data on equal parts by processor count
    data_chunks = chunkify(open_data, 8)

    # Run mapreduce
    print("Start mapreduce...")
    start = time()
    with Pool(8) as pool:
        mapped = pool.map(chunks_mapper, data_chunks)
        reduced = reduce(reducer, mapped)
        print("Result:\n{}".format(reduced.most_common(10)))
    finish = time()
    mapreduce_time = finish - start
    print("Finish mapreduce.\nTime: {}".format(mapreduce_time))
    print("Total time: {}".format(read_time + mapreduce_time))
