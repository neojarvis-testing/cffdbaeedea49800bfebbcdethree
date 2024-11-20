import unittest
from pyspark import SparkContext
import importlib.util

class WordFrequencyAnalysisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up SparkContext for testing
        cls.sc = SparkContext("local", "WordFrequencyAnalysisTest")

    @classmethod
    def tearDownClass(cls):
        # Stop SparkContext after all tests
        cls.sc.stop()

    def verify_function_exists(self, module_name, function_name):
        """Checks if a function exists in a given module."""
        spec = importlib.util.find_spec(module_name)
        if not spec:
            self.fail(f"Module '{module_name}' is missing.")
        module = importlib.import_module(module_name)
        if not hasattr(module, function_name):
            self.fail(f"Function '{function_name}' is missing in module '{module_name}'.")

    def test_clean_and_tokenize(self):
        self.verify_function_exists("data_cleaning", "clean_and_tokenize")
        from data_cleaning import clean_and_tokenize

        # Simulated input RDD
        text_data = ["Hello world!", "PySpark is amazing.", "Hello, PySpark users!"]
        text_rdd = self.sc.parallelize(text_data)

        # Expected output
        expected_tokens = ["hello", "world", "pyspark", "is", "amazing", "hello", "pyspark", "users"]

        # Call the function
        result_rdd = clean_and_tokenize(text_rdd)

        # Collect and verify results
        result = result_rdd.collect()
        self.assertEqual(sorted(result), sorted(expected_tokens))

    def test_remove_stopwords(self):
        self.verify_function_exists("data_cleaning", "remove_stopwords")
        from data_cleaning import remove_stopwords

        # Simulated input RDD
        words_data = ["hello", "world", "pyspark", "is", "amazing", "hello", "pyspark", "users"]
        words_rdd = self.sc.parallelize(words_data)

        # Stopwords list
        stopwords = ["is", "amazing"]

        # Expected output
        expected_words = ["hello", "world", "pyspark", "hello", "pyspark", "users"]

        # Call the function
        result_rdd = remove_stopwords(words_rdd, stopwords)

        # Collect and verify results
        result = result_rdd.collect()
        self.assertEqual(sorted(result), sorted(expected_words))

    def test_calculate_word_frequencies(self):
        self.verify_function_exists("analysis", "calculate_word_frequencies")
        from analysis import calculate_word_frequencies

        # Simulated input RDD
        words_data = ["hello", "world", "hello", "pyspark", "pyspark", "pyspark"]
        words_rdd = self.sc.parallelize(words_data)

        # Expected output
        expected_frequencies = [("hello", 2), ("world", 1), ("pyspark", 3)]

        # Call the function
        result_rdd = calculate_word_frequencies(words_rdd)

        # Collect and verify results
        result = result_rdd.collect()
        self.assertEqual(sorted(result), sorted(expected_frequencies))

    def test_get_top_n_words(self):
        self.verify_function_exists("analysis", "get_top_n_words")
        from analysis import get_top_n_words

        # Simulated input RDD
        word_freq_data = [("hello", 2), ("world", 1), ("pyspark", 3)]
        word_freq_rdd = self.sc.parallelize(word_freq_data)

        # Expected output
        expected_top_words = [("pyspark", 3), ("hello", 2)]

        # Call the function
        result = get_top_n_words(word_freq_rdd, 2)

        # Verify results
        self.assertEqual(result, expected_top_words)


if __name__ == "__main__":
    unittest.main()
