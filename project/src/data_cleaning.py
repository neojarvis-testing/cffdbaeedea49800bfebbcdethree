import string

def clean_and_tokenize(text_rdd):
    """Cleans and tokenizes text data."""
    # Remove punctuation, convert to lowercase, and split into words
    return text_rdd.flatMap(
        lambda line: line.translate(str.maketrans("", "", string.punctuation)).lower().split()
    )

def remove_stopwords(words_rdd, stopwords_list):
    """Removes stop words from the tokenized words."""
    return words_rdd.filter(lambda word: word not in stopwords_list)
