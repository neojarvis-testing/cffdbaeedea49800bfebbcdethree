import os
from src.data_loading import load_text_data, load_stopwords
from src.data_cleaning import clean_and_tokenize, remove_stopwords
from src.analysis import calculate_word_frequencies, get_top_n_words
from pyspark import SparkContext

def save_to_file(output_dir, filename, data):
    """Saves data to a file in the output directory."""
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, filename), "w") as f:
        for item in data:
            f.write(f"{item[0]}: {item[1]}\n")

def main():
    sc = SparkContext("local", "WordFrequencyAnalysis")

    try:
        # Load data
        text_rdd = load_text_data(sc, "data/text_data.txt")
        stopwords_list = load_stopwords(sc, "data/stopwords.txt")

        # Clean and tokenize text
        words_rdd = clean_and_tokenize(text_rdd)

        # Remove stop words
        filtered_words_rdd = remove_stopwords(words_rdd, stopwords_list)

        # Analyze word frequencies
        word_freq_rdd = calculate_word_frequencies(filtered_words_rdd)

        # Save word frequencies to a file
        word_freq_output_dir = "output"
        word_freq_output_file = "word_frequencies.txt"
        word_freq_rdd.saveAsTextFile(os.path.join(word_freq_output_dir, "raw_word_frequencies"))  # Save raw RDD output
        word_freq_data = word_freq_rdd.collect()
        save_to_file(word_freq_output_dir, word_freq_output_file, word_freq_data)

        # Get and save top 10 frequent words
        top_words = get_top_n_words(word_freq_rdd, 10)
        top_words_file = "top_words.txt"
        save_to_file(word_freq_output_dir, top_words_file, top_words)

        # Display top 10 frequent words
        print("\nTop 10 Words:")
        for word, count in top_words:
            print(f"{word}: {count}")

        print("\nAnalysis completed successfully.")
        print(f"Results saved to: {word_freq_output_dir}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        sc.stop()

if __name__ == "__main__":
    main()
