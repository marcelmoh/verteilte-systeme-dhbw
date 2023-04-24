from importlib.machinery import WindowsRegistryFinder
import time
import multiprocessing as mp
from functools import reduce
import operator

def find_longest_word(text):
    longest_word = None
    length_longest_word = 0
    # TODO: Write a code that finds the longest word in the text
    #  and return the word and it's length.
    for word in text.split():
        if len(word) > length_longest_word:
            length_longest_word = len(word)
            longest_word = word
    return longest_word, length_longest_word

def mapper(texts):
    # TODO: Find length of every word in the texts.
    #  Create a Dictionary of (key, value): (Word, length of Word) pair. For eg: (Have, 4), (a, 1)
    #  Use reducer function to find word of maximum length. For eg: {beautiful, 9}
    mapped_part = map(len, texts.split())
    mapped_part = dict(zip(texts.split(), mapped_part))
    return reducer(mapped_part)

def shuffle(mappedd):
    # TODO: Convert a list of Dictionaries to a single Dictionary
    return {k:v for d in mappedd for k,v in d.items()}

def reducer(mappedd):
    # TODO: Find the maximum key value pair in a Dictionary.
    #  Return it as a new Dictionary.
    #  !!! Do not forget to use it in mapper !!!
    longest_word = max(mappedd.items(), key = operator.itemgetter(1))[0]
    length_longest_word = len(longest_word)
    return {f"{longest_word}": length_longest_word}

if __name__ == "__main__":
    times = 10_000_000 # Play with this value and observe the result
    length_of_part = 10_000
    small_data = "Have a beautiful day "
    big_data = times*small_data
    length_big_data = len(big_data)
    number_of_parts = length_big_data//length_of_part

    # Measure Time
    tic = time.perf_counter()
    longest_word, length_longest_word = find_longest_word(text=small_data)
    toc = time.perf_counter()
    print(f"Time required on small_data = {toc-tic}, Word {longest_word}: Length = {length_longest_word}")

    # Measure Time
    tic = time.perf_counter()
    longest_word, length_longest_word = find_longest_word(text=big_data)
    toc = time.perf_counter()
    print(f"Time required on big_data = {toc - tic}, Word {longest_word}: Length = {length_longest_word}")

    # MapReduce
    tic = time.perf_counter()
    # 1. Split: Dividing the data into n small parts
    data_split = (big_data[i:i + number_of_parts] for i in range(0, length_big_data, number_of_parts))

    # 2. Creating processors
    pool = mp.Pool(mp.cpu_count())

    # 3. Map
    mappedd = pool.map(mapper, data_split)

    # 4. Shuffle
    shuffled = shuffle(mappedd)

    # 5. Reduce
    reduced = reducer(shuffled)
    toc = time.perf_counter()
    print(f"Time required on big_data = {toc - tic}, Word {reduced}")
