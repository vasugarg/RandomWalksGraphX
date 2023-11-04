import sbt.Keys.libraryDependencies

ThisBuild / version := "1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"


val scalaTestVersion = "3.2.15"
val typeSafeConfigVersion = "1.4.2"
val logbackVersion = "1.2.10"
val graphVizVersion = "0.18.1"
val netBuddyVersion = "1.14.4"
val catsVersion = "2.9.0"
val apacheCommonsVersion = "2.13.0"
val jGraphTlibVersion = "1.5.2"
val scalaParCollVersion = "1.0.4"
val awsSDKVersion = "2.17.28"
val snakeYamlVersion = "2.0"
lazy val sparkVersion = "3.4.1"

lazy val commonDependencies = Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParCollVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalatestplus" %% "mockito-4-2" % "3.2.12.0-RC2" % Test,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  //"ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.slf4j" % "slf4j-api" % "2.0.5",
  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "net.bytebuddy" % "byte-buddy" % netBuddyVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.14.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.0"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.14.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.0"
)

// Root project
lazy val root = (project in file("."))
  .settings(
    name := "RandomWalksGraphX"
  )
  .aggregate(NetGraphProps)
  .dependsOn(NetGraphProps)
  .settings(
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.json4s" %% "json4s-jackson" % "3.6.6",
      "org.typelevel" %% "cats-core" % catsVersion,
      "software.amazon.awssdk" % "s3" % awsSDKVersion,
      "org.yaml" % "snakeyaml" % snakeYamlVersion
    ),
    libraryDependencies  ++= Seq("ch.qos.logback" % "logback-classic" % logbackVersion)
  )

// Subproject
lazy val NetGraphProps = (project in file("NetGraphProps"))
  .settings(
    name := "NetGraphProps",
    libraryDependencies  ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion
    )
  )

scalacOptions ++= Seq(
  "-deprecation", // emit warning and location for usages of deprecated APIs
  "--explain-types", // explain type errors in more detail
  "-feature" // emit warning and location for usages of features that should be imported explicitly
)

Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources"


compileOrder := CompileOrder.JavaThenScala
test / fork := true
run / fork := true
run / javaOptions ++= Seq(
  "-Xms8G",
  "-Xmx100G",
  "-XX:+UseG1GC"
)
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
Compile / mainClass := Some("cs441.HW2.Main")
run / mainClass := Some("cs441.HW2.Main")

val jarName = "randomwalksgraphX.jar"
assembly/assemblyJarName := jarName

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shaded.jackson.@1").inAll
)

//Merging strategies
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// Exclude the unused keys from lint check
Global / excludeLintKeys := Set(
  root / run / mainClass,
  root / test / fork
)
