name := "spark-more"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % Provided
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test