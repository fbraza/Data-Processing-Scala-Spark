# Log file processing with Scala & Spark
It is a Scala application that processes around 2.10<sup>6</sup> lines of log data and generate a report containing JSON objects of the following pattern:

```
{data: <dd-MM-yyyy>, host_ip_count_by_date: {ip_1: count_1, ip_2: count_2 ...}, uri_destination: {uri: number_of_connections}
```

## Functionality

1. Generate a report from raw log data as described above
2. Provide the possibility to package the repository as a Jar to be submitted on Spark

## Installation

This application is written in Scala and uses Spark. First check that Scala & Spark are installed on your computer ([here](https://www.scala-lang.org/download/), & [here](https://spark.apache.org/downloads.html)). Put Spark command in your PATH. For Scala prefer installation with the `sbt` compiler.

## Usage

Clone the repository in your computer. Its structure should be:

```bash
.
├── access.log.gz
├── build.sbt
├── README.md
└── src
    ├── main
    │   └── scala
    │       ├── generateReport.scala
    │       └── logProcessing.scala
    └── test
        └── scala
            └── logProcessingTest.scala
```

With the following `build.sbt` file:

```scala
name := "DSTI-JSON-LOG-REPORT"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
```

Open a terminal and run the following command:

```bash
sbt run
```

This will build, compile, run the Scala program and generate a  `logReport.txt` containing the processed log data.

```bash
# Example: in reality you will have one JSON object by line
{	"date":"2017-02-08",
	"ipCount":	
{"62.138.0.25":2,"207.46.13.129":3,"217.211.168.91":2,"23.244.68.139":3,"180.76.15.162":1,"195.22.126.193":6,"144.76.198.150":13,"52.53.171.141":1,"93.183.255.101":1,"180.76.15.150":1,"141.8.142.198":3,"178.250.244.92":1,"199.30.24.252":14,"180.76.15.11":1,"180.76.15.141":1,"66.249.66.155":1,"66.249.76.45":1,"50.0.131.119":2,"141.8.142.147":1,"207.46.13.230":1,"68.168.164.252":2,"77.75.79.101":1,"178.190.111.100":9,"207.46.13.152":2,"77.75.79.72":1,"180.76.15.140":1,"81.209.177.189":2,"77.75.78.171":1,"207.46.13.60":4,"43.229.12.12":1,"183.89.188.2":3,"178.165.130.164":44,"66.249.76.136":1,"206.169.185.30":2,"66.249.66.198":1,"149.56.83.40":97533,"151.80.18.236":8,"157.55.39.53":2,"77.75.77.101":1,"77.75.77.119":1,"23.229.104.238":1,"180.76.15.148":1,"96.8.113.69":2,"180.76.15.6":1,"62.210.162.209":1,"195.22.126.190":96,"207.46.13.219":1},
	"uriCount":
{"/administrator/index.php":97539}
}
```

## Testing

You can run the test using the following command at the root directory:

```bash
sbt test
```

> Running the test first (before doing `sbt run`) will build, compile and run the tests. 

## Packaging the app as a jar

After running the app or the tests the structure of your folder should be updated:

```bash
.
├── access.log.gz
├── build.sbt
├── project
│   ├── build.properties
│   ├── project
│   └── target
├── src
│   ├── main
│   └── test
└── target
    ├── scala-2.12
    ├── streams
    └── test-reports
```

Two new folders appeared as a result of the `sbt` build: **`project`** & **`target`**. In the project folder create a file named **`assembly.sbt`** and add the following line to it:

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
```

Once done you can add the following piece of code in you **`build.sbt`** configuration file:

```scala
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
assemblyJarName in assembly := "Log-report-scala-sbt-assembly-fatjar-1.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
```

This will permit to download the `sbt assembly` plug-in and define the merging strategy of the necessary libraries. Once done just run:

```bash
sbt assembly
```

This will then generated a `jar` file named **`Log-report-scala-sbt-assembly-fatjar-1.0.jar`** and located at **`target/scala-2.12`**

## Submitting the jar package to Spark

When the `jar` has been generated you can run it with spark using the following command:

```bash
spark-submit target/scala-2.12/Log-report-scala-sbt-assembly-fatjar-1.0.jar
```

# Author

Faouzi Braza

