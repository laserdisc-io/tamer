lazy val V = new {
  val avro4s        = "3.0.4"
  val cats          = "2.0.0"
  val `cats-effect` = "2.0.0"
  val ciris         = "1.0.2"
  val confluent     = "5.3.1"
  val doobie        = "0.8.6"
  val kafka         = "2.4.0"
  val logback       = "1.2.3"
  val `log-effect`  = "0.12.0"
  val postgres      = "42.2.9"
  val refined       = "0.9.10"
  val scalacheck    = "1.14.3"
  val scalatest     = "3.1.0"
  val silencer      = "1.4.2"
  val zio           = "1.0.0-RC17"
  val `zio-interop` = "2.0.0.0-RC10"
  val `zio-kafka`   = "0.4.1"
  val `zio-macros`  = "0.6.0"
}

lazy val D = new {
  val cats = Seq(
    "org.typelevel" %% "cats-core"   % V.cats,
    "org.typelevel" %% "cats-effect" % V.`cats-effect`
  )

  val config = Seq(
    "is.cir" %% "ciris"         % V.ciris,
    "is.cir" %% "ciris-refined" % V.ciris
  )

  val doobie = Seq(
    "org.tpolecat" %% "doobie-core"   % V.doobie,
    "org.tpolecat" %% "doobie-hikari" % V.doobie
  )

  val kafka = Seq(
    "io.confluent"     % "kafka-avro-serializer" % V.confluent,
    "org.apache.kafka" % "kafka-clients"         % V.kafka
  )

  val logs = Seq(
    "ch.qos.logback" % "logback-classic" % V.logback,
    "io.laserdisc"   %% "log-effect-fs2" % V.`log-effect`,
    "io.laserdisc"   %% "log-effect-zio" % V.`log-effect`
  )

  val postgres = Seq(
    "org.postgresql" % "postgresql" % V.postgres
  )

  val refined = Seq(
    "eu.timepit" %% "refined" % V.refined
  )

  val serialization = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s
  )

  val silencer = Seq(
    "com.github.ghik" %% "silencer-lib" % V.silencer % Provided
  )

  val tests = Seq(
    "org.scalacheck" %% "scalacheck" % V.scalacheck % Test,
    "org.scalactic"  %% "scalactic"  % V.scalatest  % Test,
    "org.scalatest"  %% "scalatest"  % V.scalatest  % Test
  )

  val zio = Seq(
    "dev.zio" %% "zio-interop-cats" % V.`zio-interop`,
    "dev.zio" %% "zio-kafka"        % V.`zio-kafka`,
    "dev.zio" %% "zio-macros-core"  % V.`zio-macros`,
    "dev.zio" %% "zio-streams"      % V.zio
  )
}

inThisBuild {
  Seq(
    organization := "io.laserdisc",
    scalaVersion := "2.12.10",
    homepage := Some(url("https://github.com/laserdisc-io/tamer")),
    licenses += "MIT" -> url("http://opensource.org/licenses/MIT"),
    developers += Developer("sirocchj", "Julien Sirocchi", "julien.sirocchi@gmail.com", url("https://github.com/sirocchj")),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-Yrangepos",
      "-feature",
      "-Xfuture",
      "-Ypartial-unification",
      "-language:higherKinds",
      "-language:existentials",
      "-language:implicitConversions",
      "-unchecked",
      "-Yno-adapted-args",
      "-Xlint:_,-type-parameter-shadow",
      "-Xsource:2.13",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
      "-Xfatal-warnings",
      "-Ywarn-unused",
      "-opt-warnings",
      "-Xlint:constant",
      "-Ywarn-extra-implicit"
    ),
    resolvers += "confluent" at "https://packages.confluent.io/maven/"
  )
}

lazy val tamer = project
  .in(file("core"))
  .settings(
    name := "tamer",
    libraryDependencies ++= (D.cats ++ D.config ++ D.doobie ++ D.kafka ++ D.logs ++ D.refined ++ D.serialization ++ D.silencer ++ D.tests ++ D.zio)
      .map(_.withSources)
      .map(_.withJavadoc),
    addCompilerPlugin("com.github.ghik"  %% "silencer-plugin" % V.silencer),
    addCompilerPlugin(("org.scalamacros" % "paradise"         % "2.1.1") cross CrossVersion.full),
    Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    Test / console / scalacOptions := (Compile / console / scalacOptions).value
  )

lazy val example = project
  .in(file("example"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(tamer)
  .settings(
    libraryDependencies ++= D.postgres,
    publish / skip := true
  )

lazy val root = project
  .in(file("."))
  .aggregate(tamer, example)
  .settings(
    publish / skip := true,
    addCommandAlias("fmtCheck", ";scalafmtCheckAll;scalafmtSbtCheck"),
    addCommandAlias("fmt", ";test:scalafmtAll;scalafmtAll;scalafmtSbt;test:scalafmtAll"),
    addCommandAlias("fullBuild", ";fmtCheck;clean;test")
  )
