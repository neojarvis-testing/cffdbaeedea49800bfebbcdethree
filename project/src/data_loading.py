from pyspark import SparkContext

def load_text_data(sc, file_path):
    """Loads text data into an RDD."""
    return sc.textFile(file_path)

def load_stopwords(sc, file_path):
    """Loads stop words into an RDD."""
    stopwords_rdd = sc.textFile(file_path)
    return stopwords_rdd.collect()  # Convert to list for filtering
