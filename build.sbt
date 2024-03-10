val V = new {
  val avro4s_scala2             = "4.1.2"
  val avro4s_scala3             = "5.0.9"
  val awssdk                    = "2.25.6"
  val `cats-effect`             = "3.5.3"
  val circe                     = "0.14.6"
  val confluent                 = "7.5.3"
  val doobie                    = "1.0.0-RC5"
  val http4s                    = "0.23.26"
  val jackson                   = "2.16.2"
  val `json-schema`             = "1.14.4"
  val `jsoniter-scala`          = "2.28.4"
  val kafka                     = "3.6.1"
  val logback                   = "1.5.3"
  val `log-effect`              = "0.19.0"
  val ocisdk                    = "3.37.0"
  val postgresql                = "42.7.2"
  val `scala-collection-compat` = "2.11.0"
  val slf4j                     = "2.0.12"
  val sttp                      = "4.0.0-M10"
  val upickle                   = "3.1.4"
  val vulcan                    = "1.10.1"
  val `zio-interop`             = "23.1.0.1"
  val `zio-cache`               = "0.2.3"
  val `zio-json`                = "0.6.2"
  val `zio-kafka`               = "2.7.3"
  val `zio-nio`                 = "2.0.2"
  val `zio-oci-objectstorage`   = "0.7.1"
  val `zio-s3`                  = "0.4.2.4"
}

lazy val D = new {
  lazy val avro4s = new {
    val scala2 = "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s_scala2
    val scala3 = "com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s_scala3
  }
  val `aws-s3`        = "software.amazon.awssdk" % "s3"            % V.awssdk
  val `cats-effect`   = "org.typelevel"         %% "cats-effect"   % V.`cats-effect`
  val `circe-core`    = "io.circe"              %% "circe-core"    % V.circe
  val `circe-generic` = "io.circe"              %% "circe-generic" % V.circe
  val `circe-literal` = "io.circe"              %% "circe-literal" % V.circe
  val `circe-parser`  = "io.circe"              %% "circe-parser"  % V.circe
  val `doobie-core`   = "org.tpolecat"          %% "doobie-core"   % V.doobie
  val `doobie-hikari` = "org.tpolecat"          %% "doobie-hikari" % V.doobie
  val `embedded-kafka` =
    "io.github.embeddedkafka" %% "embedded-kafka" % V.kafka excludeAll ("org.scala-lang.modules" % "scala-collection-compat_2.13")
  val `embedded-kafka-schema-registry` =
    "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % V.confluent excludeAll ("com.github.everit-org.json-schema" % "org.everit.json.schema", "org.scala-lang.modules" % "scala-collection-compat_2.13", "org.slf4j" % "slf4j-log4j12")
  val `http4s-circe`        = "org.http4s"                            %% "http4s-circe"           % V.http4s
  val `http4s-dsl`          = "org.http4s"                            %% "http4s-dsl"             % V.http4s
  val `http4s-ember-server` = "org.http4s"                            %% "http4s-ember-server"    % V.http4s
  val `jackson-annotations` = "com.fasterxml.jackson.core"             % "jackson-annotations"    % V.jackson
  val `jackson-core`        = "com.fasterxml.jackson.core"             % "jackson-core"           % V.jackson
  val `jackson-databind`    = "com.fasterxml.jackson.core"             % "jackson-databind"       % V.jackson
  val `json-schema`         = "com.github.everit-org.json-schema"      % "org.everit.json.schema" % V.`json-schema`
  val `jsoniter-scala-core` = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"    % V.`jsoniter-scala`
  val `jul-to-slf4j`        = "org.slf4j"                              % "jul-to-slf4j"           % V.slf4j
  val `kafka-clients`       = "org.apache.kafka"                       % "kafka-clients"          % V.kafka
  val `kafka-schema-registry-client` = "io.confluent" % "kafka-schema-registry-client" % V.confluent excludeAll ("org.apache.kafka", "kafka-clients")
  val `log4j-over-slf4j`             = "org.slf4j"    % "log4j-over-slf4j"             % V.slf4j
  val `logback-classic`            = "ch.qos.logback"                 % "logback-classic"            % V.logback
  val `log-effect-zio`             = "io.laserdisc"                  %% "log-effect-zio"             % V.`log-effect`
  val `oci-java-sdk-objectstorage` = "com.oracle.oci.sdk"             % "oci-java-sdk-objectstorage" % V.ocisdk
  val postgresql                   = "org.postgresql"                 % "postgresql"                 % V.postgresql
  val `scala-collection-compat`    = "org.scala-lang.modules"        %% "scala-collection-compat"    % V.`scala-collection-compat`
  val `sttp-upickle`               = "com.softwaremill.sttp.client4" %% "upickle"                    % V.sttp
  val `sttp-zio`                   = "com.softwaremill.sttp.client4" %% "zio"                        % V.sttp
  val upickle                      = "com.lihaoyi"                   %% "upickle"                    % V.upickle
  val vulcan                       = "com.github.fd4s"               %% "vulcan"                     % V.vulcan
  val `vulcan-generic`             = "com.github.fd4s"               %% "vulcan-generic"             % V.vulcan
  val `zio-cache`                  = "dev.zio"                       %% "zio-cache"                  % V.`zio-cache`
  val `zio-interop-cats`           = "dev.zio"                       %% "zio-interop-cats"           % V.`zio-interop`
  val `zio-json`                   = "dev.zio"                       %% "zio-json"                   % V.`zio-json`
  val `zio-kafka`                  = "dev.zio"                       %% "zio-kafka"                  % V.`zio-kafka`
  val `zio-nio`                    = "dev.zio"                       %% "zio-nio"                    % V.`zio-nio`
  val `zio-oci-objectstorage`      = "io.laserdisc"                  %% "zio-oci-objectstorage"      % V.`zio-oci-objectstorage`
  val `zio-s3`                     = "dev.zio"                       %% "zio-s3"                     % V.`zio-s3`
}

enablePlugins(ZioSbtEcosystemPlugin)

inThisBuild(
  Seq(
    name               := "Tamer",
    zioVersion         := "2.0.21",
    organization       := "io.laserdisc",
    scalaVersion       := scala213.value,
    crossScalaVersions := Seq(scala213.value, scala3.value),
    homepage           := Some(url("https://github.com/laserdisc-io/tamer")),
    Test / fork        := true,
    licenses += "MIT"  -> url("http://opensource.org/licenses/MIT"),
    developers += Developer("sirocchj", "Julien Sirocchi", "julien.sirocchi@gmail.com", url("https://github.com/sirocchj")),
    resolvers ++= Seq("confluent" at "https://packages.confluent.io/maven/", "jitpack" at "https://jitpack.io")
  )
)

lazy val core = project
  .in(file("core"))
  .settings(enableZIO(enableStreaming = true))
  .settings(
    name := "tamer-core",
    libraryDependencies ++= Seq(
      D.`jackson-annotations`,
      D.`jackson-core`,
      D.`jackson-databind`,
      D.`kafka-clients`,
      D.`log-effect-zio`,
      D.`sttp-upickle`,
      D.`sttp-zio`,
      D.upickle,
      D.`zio-cache`,
      D.`zio-kafka`,
      // optional dependencies
      D.`circe-parser`        % Optional,
      D.`jsoniter-scala-core` % Optional,
      D.vulcan                % Optional,
      D.`zio-json`            % Optional,
      // test dependencies
      D.`embedded-kafka`                 % Test,
      D.`embedded-kafka-schema-registry` % Test,
      D.`json-schema`                    % Test,
      D.`jul-to-slf4j`                   % Test,
      D.`log4j-over-slf4j`               % Test,
      D.`logback-classic`                % Test
    ),
    addDependenciesOn("3")(D.`scala-collection-compat`                       % Test),
    addDependenciesOnOrElse("3")(D.avro4s.scala3 % Optional)(D.avro4s.scala2 % Optional)
  )

lazy val db = project
  .in(file("db"))
  .dependsOn(core)
  .settings(
    name := "tamer-db",
    libraryDependencies ++= Seq(
      D.`doobie-core`,
      D.`doobie-hikari`,
      D.`zio-interop-cats`
    )
  )

lazy val ociObjectStorage = project
  .in(file("oci-objectstorage"))
  .dependsOn(core)
  .settings(
    name := "tamer-oci-objectstorage",
    libraryDependencies ++= Seq(
      D.`oci-java-sdk-objectstorage`,
      D.`zio-oci-objectstorage`
    )
  )

lazy val s3 = project
  .in(file("s3"))
  .dependsOn(core)
  .settings(enableZIO())
  .settings(
    name := "tamer-s3",
    libraryDependencies ++= Seq(
      D.`aws-s3`,
      D.`zio-nio`, // zio-s3 brings scala-collection-compat_2.13 in scala3 build
      D.`zio-s3`
    )
  )

lazy val rest = project
  .in(file("rest"))
  .dependsOn(core % "compile->compile;test->compile,test")
  .settings(
    name := "tamer-rest",
    libraryDependencies ++= Seq(
      D.`sttp-zio`,
      // test dependencies
      D.`circe-core`          % Test,
      D.`circe-generic`       % Test,
      D.`circe-parser`        % Test,
      D.`http4s-circe`        % Test,
      D.`http4s-dsl`          % Test,
      D.`http4s-ember-server` % Test,
      D.`vulcan-generic`      % Test,
      D.`zio-interop-cats`    % Test
    )
  )

lazy val example = project
  .in(file("example"))
  .enablePlugins(JavaAppPackaging)
  .dependsOn(core, db, ociObjectStorage, rest, s3)
  .settings(
    libraryDependencies ++= Seq(
      D.`circe-literal`,
      D.`http4s-circe`,
      D.`http4s-dsl`,
      D.`http4s-ember-server`,
      D.`logback-classic`,
      D.postgresql,
      D.`scala-collection-compat`,
      D.`vulcan-generic`
    ),
    publish / skip := true
  )

lazy val tamer = project
  .in(file("."))
  .aggregate(core, example, db, ociObjectStorage, rest, s3)
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
