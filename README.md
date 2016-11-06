# Scala-Machine-Learning
Scala Machine Learning Code

Geo Life Tracker data clustering.
The dataset is available here: https://www.microsoft.com/en-us/download/details.aspx?id=52367
It contains GPS tracks (about 24 million points) which we load into MongoDB using the WGS 84 point type.
This allows us to run geo-queries on it.

Then we use Spark and Spark ML to process the data and do K-Means clustering.

We then print out the cluster centres (longitude and latitude) and plot them on a map.

The full 24 million set with ~ 2000 centres will take a long time.
On a i7 6th Gen, 16GB RAM and 7200RPM HDD it took about 2 hrs.
