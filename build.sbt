import sbt.addCommandAlias

lazy val scala_212 = "2.12.13"
lazy val scala_213 = "2.13.4"

lazy val V = new {
  val avro4s        = "4.0.7"
  val cats          = "2.5.0"
  val ciris         = "1.2.1"
  val confluent     = "6.1.0"
  val doobie        = "0.12.1"
  val `json-schema` = "1.12.2"
  val kafka         = "2.7.0"
  val logback       = "1.2.3"
  val `log-effect`  = "0.14.1"
  val ociSdk        = "1.36.0"
  val postgres      = "42.2.19"
  val refined       = "0.9.23"
  val scalacheck    = "1.15.3"
  val scalatest     = "3.2.7"
  val silencer      = "1.7.3"
  val sttp          = "3.2.3"
  val zio           = "1.0.6"
  val `zio-kafka`   = "0.14.0"
  val `zio-oci-os`  = "0.2.0"
  val `zio-s3`      = "0.3.1"

  val http4s = "1.0.0-M10" // last compatible with CE 2.3

  private val `cats-effect-version` = "2.3"
  val `cats-effect`                 = s"${`cats-effect-version`}.3"
  val `zio-interop`                 = s"${`cats-effect-version`}.1.0"

  val circeVersion = "0.13.0"
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

  val ociObjectStorage = Seq(
    "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % V.ociSdk,
    "io.laserdisc"      %% "zio-oci-objectstorage"      % V.`zio-oci-os`
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
    "org.scalacheck"                   %% "scalacheck"                     % V.scalacheck    % Test,
    "org.scalactic"                    %% "scalactic"                      % V.scalatest     % Test,
    "org.scalatest"                    %% "scalatest"                      % V.scalatest     % Test,
    "io.github.embeddedkafka"          %% "embedded-kafka"                 % V.kafka         % Test,
    "io.github.embeddedkafka"          %% "embedded-kafka-schema-registry" % V.confluent     % Test excludeAll ("com.github.everit-org.json-schema" % "org.everit.json.schema"),
    "com.github.everit-org.json-schema" % "org.everit.json.schema"         % V.`json-schema` % Test
  )

  val zio = Seq(
    "dev.zio" %% "zio-interop-cats" % V.`zio-interop`,
    "dev.zio" %% "zio-kafka"        % V.`zio-kafka`,
    "dev.zio" %% "zio-streams"      % V.zio,
    "dev.zio" %% "zio-test"         % V.zio,
    "dev.zio" %% "zio-test-sbt"     % V.zio
  )

  val sttp = Seq(
    "com.softwaremill.sttp.client3" %% "httpclient-backend-zio" % V.sttp
  )

  val http4s = Seq(
    "org.http4s" %% "http4s-dsl"          % V.http4s,
    "org.http4s" %% "http4s-blaze-server" % V.http4s,
    "org.http4s" %% "http4s-blaze-client" % V.http4s
  )

  val circe = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % V.circeVersion)
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
  resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven/", "jitpack" at "https://jitpack.io"),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  Test / fork := true
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
    Test / console / scalacOptions := (Compile / console / scalacOptions).value
  )

lazy val doobie = project
  .in(file("doobie"))
  .dependsOn(tamer)
  .settings(commonSettings)
  .settings(
    name := "tamer-doobie",
    libraryDependencies ++= D.doobie
  )

lazy val ociObjectStorage = project
  .in(file("oci-objectstorage"))
  .dependsOn(tamer)
  .settings(commonSettings)
  .settings(
    name := "tamer-oci-objectstorage",
    libraryDependencies ++= D.ociObjectStorage,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val s3 = project
  .in(file("s3"))
  .dependsOn(tamer)
  .settings(commonSettings)
  .settings(
    name := "tamer-s3",
    libraryDependencies ++= D.s3
  )

lazy val rest = project
  .in(file("rest"))
  .dependsOn(tamer % "compile->compile;test->compile,test")
  .settings(commonSettings)
  .settings(
    name := "tamer-rest",
    libraryDependencies ++= D.sttp,
    libraryDependencies ++= D.http4s.map(_ % Test),
    libraryDependencies ++= D.circe.map(_ % Test),
    libraryDependencies ++= D.tests
  )

lazy val example = project
  .in(file("example"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(tamer, doobie, ociObjectStorage, rest, s3)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= D.postgres,
    publish / skip := true
  )

lazy val root = project
  .in(file("."))
  .aggregate(tamer, example, doobie, ociObjectStorage, rest, s3)
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
