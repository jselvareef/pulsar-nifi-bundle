# Apache NiFi - Processor for Apache Pulsar

## Compatibility

| Bundle version | NiFi | Pulsar client | Java |
|---|---|---|---|
| `2.9.0-batchfix.1` | 2.9.0 | 4.2.2 | 21 |
| `2.1.0` | 2.1.0 | 3.3.7 | 21 |

The bundle version tracks the NiFi platform version it is built for; each release
line targets one Pulsar client major. See [VERSIONING.md](VERSIONING.md) for the
full scheme, branching model, and release process.

## Fork build `2.9.0-batchfix.1`

This build fixes `ConsumePulsar` / `ConsumePulsarRecord` ignoring **Consumer Message Batch Size**
(one FlowFile per message instead of up to N messages per FlowFile). It is built from the upstream
`v2.9.0` tag (NiFi 2.9.0, Pulsar client 4.2.2, Java 21) plus upstream commit `f8a15fb` (which makes the
JUnit 4 suite actually run) and the fix itself. The version follows the `<nifi.version>[.<revision>]`
scheme of [VERSIONING.md](VERSIONING.md) with a `-batchfix.1` qualifier so the artifacts cannot be
confused with the upstream `2.9.0` release. Both NARs must always be installed with the **same**
version: `nifi-pulsar-nar` declares `nifi-pulsar-client-service-nar` as its parent NAR.

Only the **Mapped FlowFile Attributes** decide whether consecutive messages share a FlowFile. The
Pulsar metadata attributes are derived from the whole batch:

| Attribute | FlowFile with 1 message | FlowFile with N > 1 messages |
|---|---|---|
| `pulsar.message.id` | id of the message (unchanged) | not set |
| `pulsar.message.id.first` / `pulsar.message.id.last` | id of the message | ids of the first / last message |
| `pulsar.property.<name>` | every message property | only the properties whose value is identical in all N messages |
| `message.count` (`record.count` for records) | `1` | `N` (number of records) |

## How to build

To build the NAR files using Maven, just run the following commands. The first one makes sure that you are using Java 
version 21, which is necessary since NiFi 2.x uses this version.

```
export JAVA_HOME=`/usr/libexec/java_home -v 21`
mvn clean package
```

This will also generate a Docker image inside your local docker daemon with the tag `streamnative/nifi`

*Note: Currently, this command will load NAR files that were build using the default NiFi, Pulsar, and Java versions
into the lib folder of the NiFi container for testing. Therefore, if you need to test artifacts built using a
different version of these libraries, then you will first need to copy those NAR artifacts into the docker/lib folder *BEFORE* building
the Docker image.

## How to test

A Dockerfile has been included in the project that can be used to test the Processor locally, and can be started with the following command:

```
docker run --name nifi -d -p 8443:8443 \
-e SINGLE_USER_CREDENTIALS_USERNAME=admin \
-e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB \
streamnative/nifi
```

See the [documentation](https://hub.docker.com/r/apache/nifi) on the base image for more configuration options

Visit https://localhost:8443/nifi/#/login and enter the username and password you provided in the docker command.

## How to debug

The JVM Debugger can be enabled by setting the environment variable NIFI_JVM_DEBUGGER to any value when running the docker image, e.g.

```
docker run -d --name nifi \
-v /Users/david/Downloads/nifi-test/:/nifi-test
-p 8443:8443 -p 8000:8000 \
-e NIFI_JVM_DEBUGGER=true
-e SINGLE_USER_CREDENTIALS_USERNAME=admin
-e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB
streamnative/nifi
```

## References
https://stackoverflow.com/questions/55811413/is-it-possible-to-debug-apache-nifi-custom-processor
