val scala_212 = "2.12.18"
val scala_213 = "2.13.12"

val V = new {
  val avro4s             = "4.1.1"
  val awsSdk             = "2.21.2"
  val `cats-effect`      = "3.5.2"
  val circe              = "0.14.6"
  val ciris              = "3.3.0"
  val confluent          = "7.5.1"
  val doobie             = "1.0.0-RC4"
  val http4s             = "0.23.23"
  val jackson            = "2.15.3"
  val `jackson-databind` = "2.15.3"
  val `json-schema`      = "1.14.3"
  val `jsoniter-scala`   = "2.24.1"
  val kafka              = "3.6.0"
  val logback            = "1.4.11"
  val `log-effect`       = "0.17.0"
  val ociSdk             = "3.26.0"
  val postgres           = "42.6.0"
  val `scala-compat`     = "2.11.0"
  val slf4j              = "2.0.9"
  val sttp               = "3.9.0"
  val vulcan             = "1.9.0"
  val zio                = "2.0.18"
  val `zio-interop`      = "23.0.0.8"
  val `zio-json`         = "0.6.2"
  val `zio-kafka`        = "2.5.0"
  val `zio-oci-os`       = "0.6.0"
  val `zio-s3`           = "0.4.2.4"
}

val flags = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-opt-warnings",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint:_,-infer-any,-type-parameter-shadow",
  "-Xlint:constant",
  "-Xsource:2.13",
  "-Yrangepos",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused:-nowarn"
)

def versionDependent(scalaVersion: String) =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, major)) if major >= 13 =>
      flags ++ Seq(
        "-Wconf:any:error",
        "-Xlint:-byname-implicit",
        "-Ymacro-annotations"
      )
    case _ =>
      flags ++ Seq(
        "-Xfuture",
        "-Xlint:by-name-right-associative",
        "-Xlint:unsound-match",
        "-Yno-adapted-args",
        "-Ypartial-unification",
        "-Ywarn-inaccessible",
        "-Ywarn-nullary-override",
        "-Ywarn-nullary-unit"
      )
  }

lazy val baseSettings = Seq(
  organization       := "io.laserdisc",
  scalaVersion       := scala_213,
  crossScalaVersions := Seq(scala_212, scala_213),
  homepage           := Some(url("https://github.com/laserdisc-io/tamer")),
  licenses += "MIT"  -> url("http://opensource.org/licenses/MIT"),
  developers += Developer("sirocchj", "Julien Sirocchi", "julien.sirocchi@gmail.com", url("https://github.com/sirocchj")),
  scalacOptions ++= versionDependent(scalaVersion.value)
)

lazy val commonSettings = baseSettings ++ Seq(
  resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven/", "jitpack" at "https://jitpack.io"),
  libraryDependencies ++= Seq(
    "dev.zio" %% "zio"          % V.zio,
    "dev.zio" %% "zio-test"     % V.zio % Test,
    "dev.zio" %% "zio-test-sbt" % V.zio % Test
  ),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  Test / fork                    := true
)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "tamer-core",
    libraryDependencies ++= Seq(
      "com.github.fd4s"                       %% "vulcan"                       % V.vulcan           % Optional,
      "com.fasterxml.jackson.core"             % "jackson-annotations"          % V.jackson,
      "com.fasterxml.jackson.core"             % "jackson-core"                 % V.jackson,
      "com.fasterxml.jackson.core"             % "jackson-databind"             % V.`jackson-databind`,
      "dev.zio"                               %% "zio-interop-cats"             % V.`zio-interop`,
      "dev.zio"                               %% "zio-kafka"                    % V.`zio-kafka`,
      "dev.zio"                               %% "zio-streams"                  % V.zio,
      "io.confluent"                           % "kafka-schema-registry-client" % V.confluent excludeAll ("org.apache.kafka", "kafka-clients"),
      "io.laserdisc"                          %% "log-effect-zio"               % V.`log-effect`,
      "is.cir"                                %% "ciris"                        % V.ciris,
      "org.apache.kafka"                       % "kafka-clients"                % V.kafka,
      "org.typelevel"                         %% "cats-effect"                  % V.`cats-effect`,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"          % V.`jsoniter-scala` % Optional,
      "com.sksamuel.avro4s"                   %% "avro4s-core"                  % V.avro4s           % Optional,
      "dev.zio"                               %% "zio-json"                     % V.`zio-json`       % Optional,
      "io.circe"                              %% "circe-parser"                 % V.circe            % Optional,
      "ch.qos.logback"                         % "logback-classic"              % V.logback          % Test,
      "com.github.everit-org.json-schema"      % "org.everit.json.schema"       % V.`json-schema`    % Test,
      "io.github.embeddedkafka"               %% "embedded-kafka"               % V.kafka            % Test,
      "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % V.confluent % Test excludeAll ("com.github.everit-org.json-schema" % "org.everit.json.schema", "org.slf4j" % "slf4j-log4j12"),
      "org.slf4j" % "jul-to-slf4j"     % V.slf4j % Test,
      "org.slf4j" % "log4j-over-slf4j" % V.slf4j % Test
    )
  )

lazy val db = project
  .in(file("db"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-db",
    libraryDependencies ++= Seq(
      "org.tpolecat" %% "doobie-core"   % V.doobie,
      "org.tpolecat" %% "doobie-hikari" % V.doobie
    )
  )

lazy val ociObjectStorage = project
  .in(file("oci-objectstorage"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-oci-objectstorage",
    libraryDependencies ++= Seq(
      "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % V.ociSdk,
      "io.laserdisc"      %% "zio-oci-objectstorage"      % V.`zio-oci-os`
    )
  )

lazy val s3 = project
  .in(file("s3"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-s3",
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio-s3" % V.`zio-s3`,
      "software.amazon.awssdk" % "s3"     % V.awsSdk
    )
  )

lazy val rest = project
  .in(file("rest"))
  .dependsOn(core % "compile->compile;test->compile,test")
  .settings(commonSettings)
  .settings(
    name := "tamer-rest",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client3" %% "zio"                 % V.sttp,
      "com.github.fd4s"               %% "vulcan-generic"      % V.vulcan % Test,
      "io.circe"                      %% "circe-core"          % V.circe  % Test,
      "io.circe"                      %% "circe-generic"       % V.circe  % Test,
      "io.circe"                      %% "circe-parser"        % V.circe  % Test,
      "org.http4s"                    %% "http4s-circe"        % V.http4s % Test,
      "org.http4s"                    %% "http4s-ember-server" % V.http4s % Test,
      "org.http4s"                    %% "http4s-dsl"          % V.http4s % Test
    )
  )

lazy val example = project
  .in(file("example"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(core, db, ociObjectStorage, rest, s3)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback"          % "logback-classic"         % V.logback,
      "com.github.fd4s"        %% "vulcan-generic"          % V.vulcan,
      "io.circe"               %% "circe-literal"           % V.circe,
      "org.http4s"             %% "http4s-circe"            % V.http4s,
      "org.http4s"             %% "http4s-ember-server"     % V.http4s,
      "org.http4s"             %% "http4s-dsl"              % V.http4s,
      "org.postgresql"          % "postgresql"              % V.postgres,
      "org.scala-lang.modules" %% "scala-collection-compat" % V.`scala-compat`
    ),
    publish / skip := true
  )

lazy val tamer = project
  .in(file("."))
  .aggregate(core, example, db, ociObjectStorage, rest, s3)
  .settings(baseSettings)
  .settings(
    publish / skip := true,
    addCommandAlias("fmtCheck", "scalafmtCheckAll; scalafmtSbtCheck"),
    addCommandAlias("fmt", "scalafmtAll; scalafmtSbt"),
    addCommandAlias("fullTest", "clean; test"),
    addCommandAlias(
      "setReleaseOptions",
      """set scalacOptions ++= Seq("-opt:l:method", "-opt:l:inline", "-opt-inline-from:tamer.**", "-opt-inline-from:<sources>")"""
    ),
    addCommandAlias("releaseIt", "clean; setReleaseOptions; session list; compile; ci-release")
  )
