ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "boruvka_spark",

    libraryDependencies ++= Seq(

      "org.apache.spark" %% "spark-core" % "3.5.1",

      "org.apache.spark" %% "spark-graphx" % "3.5.1",

      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    ),
    
    fork := true,

    javaOptions ++= Seq(
      "-Xms1g",
      "-Xmx4g",
      "-XX:+UseG1GC"
    )
  )
