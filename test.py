import pyspark
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)

txt = sc.textFile('test.txt')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
with open('out.txt', 'w')as file:
    file.write(txt.count())
    file.write(python_lines.count())

# Finally, let Spark know that the job is done.
sc.stop()
