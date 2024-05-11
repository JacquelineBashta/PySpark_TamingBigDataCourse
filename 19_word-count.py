from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
read_data = sc.textFile("19_Book.txt").map(lambda x: x.lower())


def extract_words(line):
    # Define regex pattern to match words ignoring case
    pattern = r'\b\w+\b'
    matches = re.findall(pattern, line)
    return matches


my_data_2 = (read_data
             .flatMap(extract_words)
             .map(lambda x: (x, 1))
             .reduceByKey(lambda x, y: x+y)
             .sortBy(lambda x: x[1])
             .collect()
             )

for word in my_data_2:
    cleanWord = word[0].encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(word[1]))
