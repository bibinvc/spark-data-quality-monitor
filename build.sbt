// Optional: build with sbt instead of scripts/build.sh.
// Spark is "provided": it comes from the Spark install or the Dataproc runtime.
ThisBuild / scalaVersion := "2.13.16"
lazy val root = (project in file("."))
  .settings(
    name := "spark-data-quality-monitor",
    version := "0.1.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided"
  )
