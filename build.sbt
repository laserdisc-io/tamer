val scala_212 = "2.12.15"
val scala_213 = "2.13.8"

val V = new {
  val avro4s           = "4.0.12"
  val awsSdk           = "2.17.129"
  val `cats-effect`    = "3.3.5"
  val circe            = "0.14.1"
  val ciris            = "2.3.2"
  val confluent        = "7.0.1"
  val doobie           = "1.0.0-RC2"
  val `json-schema`    = "1.14.0"
  val `jsoniter-scala` = "2.13.3"
  val kafka            = "3.0.0"
  val logback          = "1.2.10"
  val `log-effect`     = "0.16.2"
  val ociSdk           = "2.15.0"
  val postgres         = "42.3.2"
  val `scala-compat`   = "2.6.0"
  val slf4j            = "1.7.36"
  val sttp             = "3.4.1"
  val uzhttp           = "0.2.8"
  val zio              = "1.0.13"
  val `zio-interop`    = "3.2.9.1"
  val `zio-json`       = "0.1.5"
  val `zio-kafka`      = "0.17.4"
  val `zio-oci-os`     = "0.4.0"
  val `zio-s3`         = "0.3.7"
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
  "-Xlint:_,-type-parameter-shadow",
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
      "dev.zio"                               %% "zio-interop-cats"             % V.`zio-interop`,
      "dev.zio"                               %% "zio-kafka"                    % V.`zio-kafka`,
      "dev.zio"                               %% "zio-streams"                  % V.zio,
      "io.confluent"                           % "kafka-schema-registry-client" % V.confluent,
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
      "com.softwaremill.sttp.client3" %% "httpclient-backend-zio1" % V.sttp,
      "com.sksamuel.avro4s"           %% "avro4s-core"             % V.avro4s % Test,
      "io.circe"                      %% "circe-core"              % V.circe  % Test,
      "io.circe"                      %% "circe-generic"           % V.circe  % Test,
      "io.circe"                      %% "circe-parser"            % V.circe  % Test,
      "org.polynote"                  %% "uzhttp"                  % V.uzhttp % Test
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
      "com.sksamuel.avro4s"    %% "avro4s-core"             % V.avro4s,
      "org.polynote"           %% "uzhttp"                  % V.uzhttp,
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
