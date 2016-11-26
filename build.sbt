name := "abigaurhari"

version := "1.0"

scalaVersion := "2.10.4"

// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
libraryDependencies ++=Seq( "com.databricks" % "spark-csv_2.10" % "1.5.0"  ,
                            "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
                            "org.apache.spark" % "spark-core_2.10" % "1.6.2",
                            "org.apache.spark" % "spark-streaming_2.10" % "1.6.2",
                            "org.apache.spark" % "spark-sql_2.10" % "1.6.2",
                            "com.novocode" % "junit-interface" % "0.8" % "test->default",
                            "com.typesafe" % "config" % "1.2.1"
)



    