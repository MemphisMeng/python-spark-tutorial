from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

def hasNumbers(inputString):
     return any(char.isdigit() for char in inputString)
    
lines = spark.sparkContext.textFile('/in/word_count.text')
words = lines.flatMap(lambda line: [word.strip('&(') for word in line.lower().split(" ") if hasNumbers(word) is False])
wordCounts = words.countByValue()
print(sorted(wordCounts.items(), key=lambda item: item[1], reverse=True))
