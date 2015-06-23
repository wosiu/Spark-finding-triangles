sbt package

/opt/spark-1.3.1-bin-hadoop2.6/bin/spark-submit \
  --class "CountTriangles" \
  --master local[4] \
  target/scala-2.10/count-triangles-spark_2.10-1.0.jar
