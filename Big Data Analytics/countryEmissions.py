
# Team Members - Deepika Gonela, Pandre Vamshi, Akshith Reddy Kota, Krishna Tej Alahari
# From the emission profile data of each facility,
# computing the average emission for each pollutant grouped by country in the european union
# Frameworks used here: PySpark, ran using GCP computes with similar configuration from assignment2

import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local").getOrCreate()
spark = SparkSession.builder.appName("local").getOrCreate()

# Reading the emission profiles data
myRdd = sc.textFile("emissionProfilesData.csv")

header = myRdd.first()
myRdd = myRdd.filter(lambda row: row != header)
myRdd = myRdd.map(lambda line: line.split(","))

# print(myRdd.take(3))

totalCols = 95
# Excluding rowid, facility_id, country_dode
col_to_drop = [0, 1, 93, 94]

new_rdd = myRdd.map(lambda row: (tuple(float(value) for i, value in enumerate(row) if i not in col_to_drop), row[-1]))
flat_rdd = new_rdd.map(lambda x: tuple(list(x[0]) + [x[1]]))
print(flat_rdd.take(1))

# Computing the average emission for each pollutant grouped by country
result = flat_rdd.map(lambda x: (x[-1], tuple(x[:-1]) + (1,))).reduceByKey(lambda x, y: tuple(xi + yi for xi, yi in zip(x, y))) \
    .mapValues(lambda x: tuple(xi/x[-1] if xi != 0 else 0 for xi in x[:-1]))

result = result.map(lambda x: ([x[0]] + list(x[1])))

# collecting column Names
column_names = []
with open("emissionProfilesData.csv", newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    column_names = reader.fieldnames

csvfile.close()

column_names = ['Country'] + column_names[2:-2]
print(column_names)
df = result.toDF(column_names)

# Writing to a single CSV
df.coalesce(1).write.csv("avgEmissionPerCountry.csv", header=True)

