object CompileOptions {
  // Scala
  lazy val scalaCompile = Seq(
    "-language:implicitConversions",
    "-language:postfixOps",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-encoding", "UTF-8",
    "-target:jvm-1.8",
    "-Ywarn-unused-import"
  )

  // Java
  lazy val javaCompile = Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked",
    "-source", "1.8",
    "-target", "1.8",
    "-g:vars",
    "-encoding", "UTF-8"
  )
}