name := "MySparkTest"
version := "0.1"

// 1. MUST match <scala.version> from your POM
scalaVersion := "2.13.18"

// 2. This tells SBT to look in your local C:\Users\<User>\.m2\repository
// where you installed your modified Spark
//resolvers += Resolver.mavenLocal
unmanagedBase := file("C:\\Spark\\spark-4.2.0-SNAPSHOT-bin-custom-spark\\spark-4.2.0-SNAPSHOT-bin-custom-spark\\jars")

// 3. Match the <version> and <scala.binary.version> from your POM
// Note: %% in libraryDependencies handles the _2.13 suffix automatically
//val sparkVersion = "4.2.0-SNAPSHOT"

libraryDependencies ++= Seq(
 // "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
