val scala_213 = "2.13.16"
val scala_3   = "3.3.5"

val V = new {
  val avro4s_scala2                    = "4.1.2"
  val avro4s_scala3                    = "5.0.14"
  val awssdk                           = "2.30.16"
  val `cats-effect`                    = "3.5.3"
  val circe                            = "0.14.10"
  val doobie                           = "1.0.0-RC7"
  val `embedded-kafka`                 = "3.9.0"
  val `embedded-kafka-schema-registry` = "7.8.0"
  val http4s                           = "0.23.30"
  val jackson                          = "2.18.2"
  val `json-schema`                    = "1.14.4"
  val `jsoniter-scala`                 = "2.33.2"
  val kafka                            = "3.9.0"
  val logback                          = "1.5.16"
  val `log-effect`                     = "0.19.4"
  val ocisdk                           = "3.55.1"
  val postgresql                       = "42.7.5"
  val `scala-collection-compat`        = "2.13.0"
  val slf4j                            = "2.0.16"
  val sttp                             = "4.0.0-RC1"
  val upickle                          = "4.1.0"
  val vulcan                           = "1.11.1"
  val zio                              = "2.1.15"
  val `zio-interop`                    = "23.1.0.3"
  val `zio-cache`                      = "0.2.3"
  val `zio-json`                       = "0.7.21"
  val `zio-kafka`                      = "2.10.0"
  val `zio-nio`                        = "2.0.2"
  val `zio-oci-objectstorage`          = "0.7.3"
  val `zio-s3`                         = "0.4.3"
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
    "io.github.embeddedkafka" %% "embedded-kafka" % V.`embedded-kafka` excludeAll ("org.scala-lang.modules" % "scala-collection-compat_2.13")
  val `embedded-kafka-schema-registry` =
    "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % V.`embedded-kafka-schema-registry` excludeAll (
      "com.github.everit-org.json-schema" % "org.everit.json.schema",
      "org.scala-lang.modules"            % "scala-collection-compat_2.13",
      "org.slf4j"                         % "slf4j-log4j12"
    )
  val `http4s-circe`               = "org.http4s"                            %% "http4s-circe"               % V.http4s
  val `http4s-dsl`                 = "org.http4s"                            %% "http4s-dsl"                 % V.http4s
  val `http4s-ember-server`        = "org.http4s"                            %% "http4s-ember-server"        % V.http4s
  val `jackson-annotations`        = "com.fasterxml.jackson.core"             % "jackson-annotations"        % V.jackson
  val `jackson-core`               = "com.fasterxml.jackson.core"             % "jackson-core"               % V.jackson
  val `jackson-databind`           = "com.fasterxml.jackson.core"             % "jackson-databind"           % V.jackson
  val `json-schema`                = "com.github.everit-org.json-schema"      % "org.everit.json.schema"     % V.`json-schema`
  val `jsoniter-scala-core`        = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"        % V.`jsoniter-scala`
  val `jul-to-slf4j`               = "org.slf4j"                              % "jul-to-slf4j"               % V.slf4j
  val `kafka-clients`              = "org.apache.kafka"                       % "kafka-clients"              % V.kafka
  val `log4j-over-slf4j`           = "org.slf4j"                              % "log4j-over-slf4j"           % V.slf4j
  val `logback-classic`            = "ch.qos.logback"                         % "logback-classic"            % V.logback
  val `log-effect-zio`             = "io.laserdisc"                          %% "log-effect-zio"             % V.`log-effect`
  val `oci-java-sdk-objectstorage` = "com.oracle.oci.sdk"                     % "oci-java-sdk-objectstorage" % V.ocisdk
  val postgresql                   = "org.postgresql"                         % "postgresql"                 % V.postgresql
  val `scala-collection-compat`    = "org.scala-lang.modules"                %% "scala-collection-compat"    % V.`scala-collection-compat`
  val `sttp-upickle`               = "com.softwaremill.sttp.client4"         %% "upickle"                    % V.sttp
  val `sttp-zio`                   = "com.softwaremill.sttp.client4"         %% "zio"                        % V.sttp
  val upickle                      = "com.lihaoyi"                           %% "upickle"                    % V.upickle
  val vulcan                       = "com.github.fd4s"                       %% "vulcan"                     % V.vulcan
  val `vulcan-generic`             = "com.github.fd4s"                       %% "vulcan-generic"             % V.vulcan
  val zio                          = "dev.zio"                               %% "zio"                        % V.zio
  val `zio-cache`                  = "dev.zio"                               %% "zio-cache"                  % V.`zio-cache`
  val `zio-interop-cats`           = "dev.zio"                               %% "zio-interop-cats"           % V.`zio-interop`
  val `zio-json`                   = "dev.zio"                               %% "zio-json"                   % V.`zio-json`
  val `zio-kafka`                  = "dev.zio"                               %% "zio-kafka"                  % V.`zio-kafka`
  val `zio-nio`                    = "dev.zio"                               %% "zio-nio"                    % V.`zio-nio`
  val `zio-oci-objectstorage`      = "io.laserdisc"                          %% "zio-oci-objectstorage"      % V.`zio-oci-objectstorage`
  val `zio-s3`                     = "dev.zio"                               %% "zio-s3"                     % V.`zio-s3`
  val `zio-streams`                = "dev.zio"                               %% "zio-streams"                % V.zio
  val `zio-test`                   = "dev.zio"                               %% "zio-test"                   % V.zio
  val `zio-test-sbt`               = "dev.zio"                               %% "zio-test-sbt"               % V.zio
}

ThisBuild / tlBaseVersion              := "0.24"
ThisBuild / tlCiReleaseBranches        := Seq("master")
ThisBuild / tlJdkRelease               := Some(11)
ThisBuild / sonatypeCredentialHost     := Sonatype.sonatypeLegacy
ThisBuild / organization               := "io.laserdisc"
ThisBuild / organizationName           := "LaserDisc"
ThisBuild / licenses                   := Seq(License.MIT)
ThisBuild / startYear                  := Some(2019)
ThisBuild / developers                 := List(tlGitHubDev("sirocchj", "Julien Sirocchi"))
ThisBuild / crossScalaVersions         := Seq(scala_213, scala_3)
ThisBuild / scalaVersion               := scala_213
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"), JavaSpec.temurin("17"), JavaSpec.temurin("21"))

ThisBuild / resolvers ++= List("confluent" at "https://packages.confluent.io/maven/", "jitpack" at "https://jitpack.io")

ThisBuild / mergifyLabelPaths    := Map.empty
ThisBuild / mergifyStewardConfig := Some(MergifyStewardConfig(action = MergifyAction.Merge(Some("squash"))))

lazy val commonSettings = Seq(
  headerEndYear := Some(2025),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, major)) if major >= 13 => Seq("-Wconf:cat=lint-infer-any:s")
      case _                               => Seq.empty
    }
  },
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  Test / fork := true
)

lazy val tamer = tlCrossRootProject
  .aggregate(core, db, `oci-objectstorage`, s3, rest, example)

lazy val core = project
  .settings(commonSettings)
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
      D.zio,
      D.`zio-cache`,
      D.`zio-kafka`,
      D.`zio-streams`,
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
      D.`logback-classic`                % Test,
      D.`zio-test`                       % Test,
      D.`zio-test-sbt`                   % Test
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) =>
          Seq(D.`scala-collection-compat` % Test, D.avro4s.scala3 % Optional)
        case _ =>
          Seq(D.avro4s.scala2 % Optional)
      }
    }
  )

lazy val db = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-db",
    libraryDependencies ++= Seq(
      D.`doobie-core`,
      D.`doobie-hikari`,
      D.`zio-interop-cats`
    )
  )

lazy val `oci-objectstorage` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-oci-objectstorage",
    libraryDependencies ++= Seq(
      D.`oci-java-sdk-objectstorage`,
      D.`zio-oci-objectstorage`
    )
  )

lazy val s3 = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "tamer-s3",
    libraryDependencies ++= Seq(
      D.`aws-s3`,
      D.zio,
      D.`zio-nio`, // zio-s3 brings scala-collection-compat_2.13 in scala3 build
      D.`zio-s3`,
      D.`zio-streams`,
      D.`zio-test`     % Test,
      D.`zio-test-sbt` % Test
    )
  )

lazy val rest = project
  .dependsOn(core % "compile->compile;test->compile,test")
  .settings(commonSettings)
  .settings(
    name := "tamer-rest",
    libraryDependencies ++= Seq(
      D.`sttp-zio`,
      D.zio,
      // test dependencies
      D.`circe-core`          % Test,
      D.`circe-generic`       % Test,
      D.`circe-parser`        % Test,
      D.`http4s-circe`        % Test,
      D.`http4s-dsl`          % Test,
      D.`http4s-ember-server` % Test,
      D.`vulcan-generic`      % Test,
      D.`zio-interop-cats`    % Test,
      D.`zio-test`            % Test,
      D.`zio-test-sbt`        % Test
    )
  )

lazy val example = project
  .enablePlugins(JavaAppPackaging, NoPublishPlugin)
  .dependsOn(core, db, `oci-objectstorage`, rest, s3)
  .settings(commonSettings)
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
    )
  )
