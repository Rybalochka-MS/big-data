from collections import Counter
from functools import reduce
from time import time
from multiprocessing import Pool


data = ['abc', 'python', 'dima', 'dima']*2


def mapper(text):
    tokens_in_text = text.split()
    return Counter(tokens_in_text)


def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1


def chunk_mapper(chunk):
    chunk_mapped = map(mapper, chunk)
    return reduce(reducer, chunk_mapped)


def chunkify(seq, size):
    return (seq[i::size] for i in range(size))


if __name__ == '__main__':
    start = time()
    with Pool(8) as pool:
        data_chunks = chunkify(data, 8)
        mapped = pool.map(chunk_mapper, data_chunks)
        reduced = reduce(reducer, mapped)
        print(reduced.most_common(10))
    finish = time()
    mapreduce_time = finish - start
    print("Finish mapreduce.\nTime: {}".format(mapreduce_time))
