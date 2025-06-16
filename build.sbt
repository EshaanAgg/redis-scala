ThisBuild / scalaVersion     := "3.7.1"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.codecrafters"
ThisBuild / organizationName := "CodeCrafters"

assembly / assemblyJarName := "redis.jar"

lazy val root = (project in file("."))
  .settings(
    name := "redis-scala",
    
    scalacOptions ++= Seq(
      "-Wunused:imports", // for OrganizeImports
      "-Wunused:all"      // for RemoveUnused
    ),

    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
    )
  )
