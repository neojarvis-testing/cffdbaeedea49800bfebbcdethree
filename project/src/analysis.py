def calculate_word_frequencies(words_rdd):
    """Calculates the frequency of each word."""
    return words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

def get_top_n_words(word_freq_rdd, n):
    """Retrieves the top N most frequent words."""
    return word_freq_rdd.sortBy(lambda x: x[1], ascending=False).take(n)
