from pyspark import SparkConf, SparkContext
import sys

# This script takes two arguments, an input and output
if len(sys.argv) != 3:
  print('Usage: ' + sys.argv[0] + ' <in> <out>')
  sys.exit(1)

input = sys.argv[1]
output = sys.argv[2]

# Set up the configuration and job context
conf = SparkConf().setAppName('AnnualWordLength')
sc = SparkContext(conf=conf)


# Read in the dataset and immediately transform all the lines in arrays
data = sc.textFile(input).map(lambda line: line.split('\t'))

# Create the 'length' dataset as mentioned above. This is done using the next two variables, and the 'length' dataset ends up in 'yearlyLength'.
yearlyLengthAll = data.map(
    lambda arr: (int(arr[1]), float(len(arr[0])) * float(arr[2]))
)
yearlyLength = yearlyLengthAll.reduceByKey(lambda a, b: a + b)

# Create the 'words' dataset as mentioned above.
yearlyCount = data.map(
    lambda arr: (int(arr[1]), float(arr[2]))
).reduceByKey(
    lambda a, b: a + b
)

# Create the 'average_length' dataset as mentioned above.
yearlyAvg = yearlyLength.join(yearlyCount).map(
    lambda tup: (tup[0], tup[1][0] / tup[1][1])
)

# Save the results in the specified output directory.
yearlyAvg.saveAsTextFile(output)

# Finally, let Spark know that the job is done.
sc.stop()
