import sbt.addCommandAlias

lazy val scala_212 = "2.12.13"
lazy val scala_213 = "2.13.4"

lazy val V = new {
  val avro4s        = "4.0.4"
  val cats          = "2.4.2"
  val `cats-effect` = "2.3.3"
  val ciris         = "1.2.1"
  val confluent     = "6.0.2"
  val doobie        = "0.10.0"
  val embeddedKafka = "2.7.0"
  val kafka         = "2.7.0"
  val logback       = "1.2.3"
  val `log-effect`  = "0.14.1"
  val postgres      = "42.2.19"
  val refined       = "0.9.21"
  val scalacheck    = "1.15.3"
  val scalatest     = "3.2.5"
  val silencer      = "1.7.3"
  val zio           = "1.0.4-2"
  val `zio-s3`      = "0.3.0"
  val `zio-interop` = "2.3.1.0"
  val `zio-kafka`   = "0.14.0"
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
    "org.apache.kafka" % "kafka-clients" % V.kafka
  )

  val avro = Seq(
    "io.confluent" % "kafka-avro-serializer" % V.confluent
  )

  val logs = Seq(
    "ch.qos.logback" % "logback-classic" % V.logback,
    "io.laserdisc"  %% "log-effect-fs2"  % V.`log-effect`,
    "io.laserdisc"  %% "log-effect-zio"  % V.`log-effect`
  )

  val postgres = Seq(
    "org.postgresql" % "postgresql" % V.postgres
  )

  val refined = Seq(
    "eu.timepit" %% "refined" % V.refined
  )

  val s3 = Seq(
    "dev.zio" %% "zio-s3" % V.`zio-s3`
  )

  val serialization = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s
  )

  val silencer = Seq(
    "com.github.ghik" %% "silencer-lib" % V.silencer % Provided cross CrossVersion.full
  )

  val tests = Seq(
    "org.scalacheck"          %% "scalacheck"     % V.scalacheck    % Test,
    "org.scalactic"           %% "scalactic"      % V.scalatest     % Test,
    "org.scalatest"           %% "scalatest"      % V.scalatest     % Test,
    "io.github.embeddedkafka" %% "embedded-kafka" % V.embeddedKafka % Test
  )

  val zio = Seq(
    "dev.zio" %% "zio-interop-cats" % V.`zio-interop`,
    "dev.zio" %% "zio-kafka"        % V.`zio-kafka`,
    "dev.zio" %% "zio-streams"      % V.zio,
    "dev.zio" %% "zio-test"         % V.zio,
    "dev.zio" %% "zio-test-sbt"     % V.zio
  )
}

lazy val flags = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-Yrangepos",
  "-feature",
  "-language:higherKinds",
  "-language:existentials",
  "-language:implicitConversions",
  "-unchecked",
  "-Xlint:_,-type-parameter-shadow",
  "-Xsource:2.13",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfatal-warnings",
  "-Ywarn-unused",
  "-opt-warnings",
  "-Xlint:constant",
  "-Ywarn-extra-implicit"
)

def versionDependent(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, major)) if major >= 13 =>
      flags ++ Seq(
        "-Wconf:any:error",
        "-Ymacro-annotations",
        "-Xlint:-byname-implicit"
      )
    case _ =>
      flags ++ Seq(
        "-Xfuture",
        "-Xlint:by-name-right-associative",
        "-Xlint:unsound-match",
        "-Yno-adapted-args",
        "-Ypartial-unification",
        "-Ywarn-inaccessible",
        "-Ywarn-infer-any",
        "-Ywarn-nullary-override",
        "-Ywarn-nullary-unit"
      )
  }

lazy val commonSettings = Seq(
  organization := "io.laserdisc",
  scalaVersion := scala_213,
  crossScalaVersions := Seq(scala_212, scala_213),
  homepage := Some(url("https://github.com/laserdisc-io/tamer")),
  licenses += "MIT" -> url("http://opensource.org/licenses/MIT"),
  developers += Developer("sirocchj", "Julien Sirocchi", "julien.sirocchi@gmail.com", url("https://github.com/sirocchj")),
  scalacOptions ++= versionDependent(scalaVersion.value),
  resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven/")
)

lazy val tamer = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "tamer-core",
    libraryDependencies ++= (D.cats ++ D.config ++ D.kafka ++ D.logs ++ D.refined ++ D.serialization ++ D.silencer ++ D.tests ++ D.zio)
      .map(_.withSources)
      .map(_.withJavadoc),
    libraryDependencies ++= D.avro,
    addCompilerPlugin("com.github.ghik" %% "silencer-plugin" % V.silencer cross CrossVersion.full),
    Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    Test / console / scalacOptions := (Compile / console / scalacOptions).value,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val doobie = project
  .in(file("doobie"))
  .dependsOn(tamer)
  .settings(commonSettings)
  .settings(
    name := "tamer-doobie",
    libraryDependencies ++= D.doobie
  )

lazy val s3 = project
  .in(file("s3"))
  .dependsOn(tamer)
  .settings(commonSettings)
  .settings(
    name := "tamer-s3",
    libraryDependencies ++= D.s3,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val example = project
  .in(file("example"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(tamer, doobie, s3)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= D.postgres,
    publish / skip := true
  )

lazy val root = project
  .in(file("."))
  .aggregate(tamer, example, doobie, s3)
  .settings(commonSettings)
  .settings(
    publish / skip := true,
    addCommandAlias("fmtCheck", ";scalafmtCheckAll;scalafmtSbtCheck"),
    addCommandAlias("fmt", ";test:scalafmtAll;scalafmtAll;scalafmtSbt;test:scalafmtAll"),
    addCommandAlias("fullTest", ";clean;test"),
    addCommandAlias(
      "setReleaseOptions",
      "set scalacOptions ++= Seq(\"-opt:l:method\", \"-opt:l:inline\", \"-opt-inline-from:laserdisc.**\", \"-opt-inline-from:<sources>\")"
    ),
    addCommandAlias("releaseIt", ";clean;setReleaseOptions;session list;compile;ci-release")
  )
