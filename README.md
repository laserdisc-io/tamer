# Tamer
[![CI](https://github.com/laserdisc-io/tamer/workflows/CI/badge.svg?branch=master)](https://github.com/laserdisc-io/tamer/actions?query=workflow%3ACI+branch%3Amaster)
[![Release](https://github.com/laserdisc-io/tamer/workflows/Release/badge.svg)](https://github.com/laserdisc-io/tamer/actions?query=workflow%3ARelease)
[![Known Vulnerabilities](https://snyk.io/test/github/laserdisc-io/tamer/badge.svg?targetFile=build.sbt)](https://snyk.io/test/github/laserdisc-io/tamer?targetFile=build.sbt)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.laserdisc/tamer_2.13/badge.svg?kill_cache=1&color=orange)](https://search.maven.org/artifact/io.laserdisc/tamer_2.13/)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Tamer is a domesticated Kafka source connector.

It puts the developer completely in control of how data is ingested and what state is preserved (in a compacted Kafka topic).
As an example, it allows for a JDBC source to pull a window of data (say, 5 minutes), starting from some time in the past, as fast as possible.
At every "pull", the developer can decide what to do next (e.g. should the window be increased/decreased? Should the pull slow down?).

Tamer currently supports two possible source types: any SQL data store Doobie can handle (e.g. Postgres, MySQL, etc) and cloud storages that "speak" the AWS S3 protocol.
Some sensible defaults are provided out of the box but many customisations are possible.

## Usage

Add one of Tamer's modules as a dependency to your project:

```
// check the current version on Maven Central (or use the badge above)
libraryDependencies += "io.laserdisc" %% "tamer-doobie" % version
```
or
```
// check the current version on Maven Central (or use the badge above)
libraryDependencies += "io.laserdisc" %% "tamer-s3" % version
```

See [here](example/src/main/scala/tamer/example/DatabaseSimple.scala) for a sample application that makes use of Tamer's Doobie module for ingesting data from a JDBC datasource.

## End to end testing

### Database module

Basic manual testing is available for the code in the example module `tamer.example.DatabaseSimple`
(and/or `tamer.example.DatabaseGeneralized`).
This code covers getting data from a synthetic Postgres database.

Make sure you have docker installed before proceeding.

From the `doobie/local` folder launch `docker-compose up` (you can enter `docker-compose down`
if you want to start from scratch). After that you should be able to access the kafka
gui from [http://localhost:8000](http://localhost:8000).

Start the `runDb.sh` program which contains some example environment variables.
If tamer works you should see messages appearing in the kafka gui.

### S3 module

Basic manual testing is available for the code in the example module `tamer.example.S3Simple`.
This code covers getting data from a synthetic S3 bucket.

Make sure you have docker installed before proceeding.

From the `s3/local` folder launch `docker-compose up` (you can enter `docker-compose down`
if you want to start from scratch). After that you should be able to access the kafka
gui from [http://localhost:8000](http://localhost:8000).

Start the `runS3.sh` program which contains some example environment variables.
If tamer works you should see messages appearing in the kafka gui.

#### Known Issues

The file list from the bucket is not synchronized with the main thread. This means that
oftentimes, when tamer resumes, you have to wait the minimum time interval between S3 fetches
before seeing some activity. This happens because the main thread may start processing before
the file list is downloaded. This should not impinge on functionality.

## License

Tamer is licensed under the **[MIT License](LICENSE)** (the "License"); you may not use this software except in
compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
