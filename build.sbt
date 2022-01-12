
name := "file-uploader"
scalaVersion := "2.13.5"
Test / fork := true

libraryDependencies ++= Dependencies.allDeps

Compile / run / mainClass := Some("uploader.Run")