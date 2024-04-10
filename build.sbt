
name := "nas_non_us_store_feat_automation"

version := "0.1.{xxbuildversionxx}"

organization := "com.pg.bigdata"
scalaVersion := "2.12.10"

resolvers ++= Seq(
  "utils" at "https://pkgs.dev.azure.com/dh-platforms-devops/app-deng-nas_us/_packaging/com.pg.bigdata/maven/v1"
)

credentials += Credentials(Path.userHome / "credentials.txt")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.1.2" % "provided",
  "com.pg.bigdata" % "utils_2.12" % "2.12.0.master_20230411.5"
)

// libraries for tests
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test"

ThisBuild / useCoursier := false
Test / parallelExecution := false

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}