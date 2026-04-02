ThisBuild / scalaVersion := "2.12.20"
ThisBuild / version      := "1.0.0-SNAPSHOT"
ThisBuild / organization := "io.github.manuzhang"

val sparkVersion   = "3.5.4"
val icebergVersion = "1.8.1"
val hadoopVersion  = "3.4.3"

lazy val root = (project in file("."))
  .settings(
    name := "iceberg-scala",
    libraryDependencies ++= Seq(
      "org.apache.spark"   %% "spark-core"                      % sparkVersion,
      "org.apache.spark"   %% "spark-sql"                       % sparkVersion,
      "org.apache.iceberg"  % "iceberg-spark-runtime-3.5_2.12"  % icebergVersion,
      "org.apache.hadoop"   % "hadoop-common"                    % hadoopVersion,
    ),
    // Fork the JVM so that sbt's classpath does not conflict with Spark's
    run / fork := true,
    // JVM flags required by Spark on JDK 17+
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ),
  )
