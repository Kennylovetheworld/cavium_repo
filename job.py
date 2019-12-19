from pyspark import SparkConf, SparkContext
import sys
import json

# This script takes two arguments, an input and output
if len(sys.argv) != 3:
  print('Usage: ' + sys.argv[0] + ' <in> <out>')
  sys.exit(1)

input = sys.argv[1]
output = sys.argv[2]

# Set up the configuration and job context
conf = SparkConf().setAppName('kennyjob')
sc = SparkContext(conf=conf)

# Read in the dataset and immediately transform all the lines in arrays
rawdata = sc.textFile(input)
data = rawdata.map(lambda line: json.loads(line.decode('utf-8')))

# Create the 'length' dataset as mentioned above. This is done using the next two variables, and the 'length' dataset ends up in 'yearlyLength'.
out = data.map(
    lambda arr: (arr['subreddit'], arr['created_utc'])
)

# Save the results in the specified output directory.
data.saveAsTextFile(output)

# Finally, let Spark know that the job is done.
sc.stop()
