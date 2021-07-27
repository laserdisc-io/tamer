# Tamer
[![CI](https://github.com/laserdisc-io/tamer/workflows/CI/badge.svg?branch=master)](https://github.com/laserdisc-io/tamer/actions?query=workflow%3ACI+branch%3Amaster)
[![Release](https://github.com/laserdisc-io/tamer/workflows/Release/badge.svg)](https://github.com/laserdisc-io/tamer/actions?query=workflow%3ARelease)
[![Known Vulnerabilities](https://snyk.io/test/github/laserdisc-io/tamer/badge.svg?targetFile=build.sbt)](https://snyk.io/test/github/laserdisc-io/tamer?targetFile=build.sbt)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.laserdisc/tamer-core_2.13/badge.svg?kill_cache=1&color=orange)](https://search.maven.org/artifact/io.laserdisc/tamer-core_2.13/)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Tamer is a domesticated Kafka source connector.

It puts the user of this library in complete control of how data is ingested and what state is preserved (in a compacted Kafka topic).
As an example, it allows for a JDBC source to pull a window of data (e.g. 5 minutes), starting from some time in the past, as fast as possible.
At every "pull", the user can decide what to do next (e.g. should the window be increased/decreased? Should the pull slow down?).

Tamer currently supports 4 possible source types:
- any SQL data store Doobie can handle (e.g. Postgres, MySQL, etc)
- any cloud storage that is AWS S3 compatible
- OCI Object Storage
- any type of REST API, with AuthN, pagination and HATEOAS support

Some sensible defaults are provided out of the box but many customisations are possible.

## Usage

Add one of Tamer's modules as a dependency to your project:

```
// check the current version on Maven Central (or use the badge above)
libraryDependencies += "io.laserdisc" %% "tamer-db"                % version
libraryDependencies += "io.laserdisc" %% "tamer-oci-objectstorage" % version
libraryDependencies += "io.laserdisc" %% "tamer-rest"              % version
libraryDependencies += "io.laserdisc" %% "tamer-s3"                % version
```

See [here](example/src/main/scala/tamer/db/DatabaseSimple.scala) for a sample application that makes use of Tamer's Db module for ingesting data from a JDBC datasource.

## End to end testing

### Database module

Basic manual testing is available for the code in the example module `tamer.db.DatabaseSimple` (and/or `tamer.db.DatabaseGeneralized`).
This code covers getting data from a synthetic Postgres database.

Make sure you have docker installed before proceeding.

From the `db/local` folder launch `docker compose up` (you can enter `docker compose down` if you want to start from scratch). After that you should be able to access the Kafka gui from [http://localhost:8000](http://localhost:8000).

Start the `runDatabaseSimple.sh` program which contains some example environment variables.
If Tamer works you should see messages appearing in the Kafka gui.

### S3 module

Basic manual testing is available for the code in the example module `tamer.s3.S3Simple`.
This code covers getting data from a synthetic S3 bucket.

Make sure you have docker installed before proceeding.

From the `s3/local` folder launch `docker compose up` (you can enter `docker compose down` if you want to start from scratch). After that you should be able to access the Kafka gui from [http://localhost:8000](http://localhost:8000).

Start the `runS3Simple.sh` program which contains some example environment variables.
If Tamer works you should see messages appearing in the Kafka gui.

## License

Tamer is licensed under the **[MIT License](LICENSE)** (the "License"); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.

