# Apache NiFi - Processor for Apache Pulsar

## Compatibility

| Bundle version | NiFi | Pulsar client | Java |
|---|---|---|---|
| `2.9.0-batchfix.6` | 2.9.0 | 4.2.2 | 21 |
| `2.1.0` | 2.1.0 | 3.3.7 | 21 |

The bundle version tracks the NiFi platform version it is built for; each release
line targets one Pulsar client major. See [VERSIONING.md](VERSIONING.md) for the
full scheme, branching model, and release process.

## Consumer FlowFile attributes

`ConsumePulsar` and `ConsumePulsarRecord` write these attributes onto every FlowFile
they emit:

| Attribute | Written by | Value |
|---|---|---|
| `message.count` | `ConsumePulsar` | number of Pulsar messages in the FlowFile |
| `record.count` | `ConsumePulsarRecord` | number of records in the FlowFile |
| `pulsar.message.id` | both | the message id — **only when the FlowFile holds exactly one message** |
| `pulsar.message.id.first` | both | id of the first message in the FlowFile |
| `pulsar.message.id.last` | both | id of the last message in the FlowFile |
| `pulsar.property.*` | both | message properties, prefixed — **only those whose value is identical in every message** |
| `topicName` | `ConsumePulsarRecord` | the logical topic the messages came from |
| `avro.schema` | `ConsumePulsarRecord` | the topic schema, when it is an AVRO schema |

A FlowFile can hold several messages: *Consumer Message Batch Size* controls how many.
Consecutive messages are appended to the same FlowFile as long as their *Mapped FlowFile
Attributes* are identical — a change in any mapped value starts a new FlowFile before the
batch size is reached. Per-message metadata (the message id, unmapped message properties)
deliberately does **not** take part in that decision, so it never splits a batch.

To force messages that differ in some property into separate FlowFiles, map that property
through *Mapped FlowFile Attributes*.

> **Attribute change since `2.9.0`:** `pulsar.message.id` and the full set of
> `pulsar.property.*` used to appear on every FlowFile, because a bug made each FlowFile
> hold exactly one message regardless of *Consumer Message Batch Size*. Now that batching
> works, a FlowFile that holds more than one message has no single message id, so
> `pulsar.message.id` is omitted there and only the properties common to the whole batch
> are set. Flows that read `${pulsar.message.id}` downstream should use
> `${pulsar.message.id.first}` / `${pulsar.message.id.last}`, or set *Consumer Message
> Batch Size* to `1` to keep one message per FlowFile.
>
> For the same reason, `pulsar.property.*` values are no longer available to
> `ConsumePulsarRecord`'s Schema Access Strategy: they are attached once the batch is
> complete, which is after the record reader and writer have been created. A schema name
> that comes from a message property has to be mapped through *Mapped FlowFile Attributes*
> instead.

### Record sets and the record schema

`ConsumePulsarRecord` writes consecutive messages with the same mapped attributes (and topic)
as one record set, using the schema the set was opened with. A message whose schema differs
starts a new record set - and a new FlowFile - just as a change in the mapped attributes does.

> **Behaviour change since `2.9.0`:** the record set used to be written with the schema of its
> **first** message. With a Record Reader that infers the schema from each message (the default
> of `JsonTreeReader`), every field that the first message of a batch did not have was silently
> dropped from the rest of the batch, and a field whose type differed made the trigger fail. Such
> batches are now split at every schema change instead, so a topic with payloads of several
> shapes produces more, smaller FlowFiles than before. If that matters more than the optional
> fields, give the reader an explicit schema (*Schema Text* or a schema registry): every message
> then resolves to the same schema and the batch stays one FlowFile.

## Publisher message metadata

`PublishPulsar` and `PublishPulsarRecord` set the message key and message properties from the
FlowFile:

| Message field | Comes from |
|---|---|
| key | the *Message Key* property; if that is not set, the FlowFile attribute `msg.key` |
| properties | the attributes named by *Mapped Message Properties* (`<property>[=<attribute>]`) |

`PublishPulsarRecord` takes the key from the record field named by *Message Key Field* instead.

> **Behaviour change since `2.9.0`:** the *Message Key* property has always documented the
> `msg.key` fallback, but it was never implemented — `getMessageKey()` read the property and
> returned nothing when it was blank. Flows that set a `msg.key` attribute without setting the
> property therefore published **unkeyed** messages. That fallback now works as documented, so
> those flows will start producing keyed messages. On a partitioned topic this changes which
> partition a message routes to, and it makes the topic compactable by that key. If you relied on
> the previous unkeyed behaviour, clear the `msg.key` attribute before the publish processor.

## Publishing to topics that have a schema

`PublishPulsar` and `PublishPulsarRecord` create their producers with
`Schema.AUTO_PRODUCE_BYTES()`, so the broker validates every payload against the schema the
topic currently carries.

| Topic | Behaviour |
|---|---|
| no schema | any content is accepted, exactly as before |
| has a schema, content matches | published normally |
| has a schema, content does not match | **routed to `failure`** with the broker's error |

> **Breaking change.** Until now the processors published with `Schema.BYTES`, which the broker
> does not validate. Content that did not match the topic's schema was accepted anyway: the
> message landed on the topic looking valid, the registered schema was left untouched, and the
> problem only appeared later, on the consumer side. A schema-aware consumer would fail to decode
> that message — and, because it could not get past it, stop consuming the topic entirely:
>
> ```
> read 1: id=sensor-1 reading=42
> read 2 FAILED: AvroRuntimeException: Malformed data. Length is negative: -62
> ```
>
> **Flows that publish content not matching their topic's schema will start routing those
> FlowFiles to `failure`.** They were previously reported as successful while producing messages
> no consumer could read, so this surfaces an existing problem rather than creating one — but it
> is a visible change in behaviour, and it needs a `failure` connection to be handled.
>
> Only topics that carry a schema are affected. If your topics have no schema — the default, and
> what every flow using these processors has relied on so far — nothing changes.
>
### Publishing records to a schema-bearing topic

`PublishPulsarRecord` has a **Message Schema Strategy** property controlling how records become messages:

| Strategy | Behaviour |
|---|---|
| `Record Writer` (default) | serialize with the configured Record Writer, as before |
| `Topic Schema` | convert each record to the topic's Avro schema and encode it the way Pulsar does |

Use `Topic Schema` when the topic carries an AVRO schema: the Record Writer's output — JSON, CSV,
Avro-with-header — is not what the broker accepts, so it is rejected. On a topic with no Avro
schema this strategy falls back to the Record Writer, so turning it on is safe either way.

JSON-schema topics are not yet encoded; only AVRO. Those still need `Record Writer` output that
happens to match.

## Fork build `2.9.0-batchfix.6`

Fork build for NiFi 2.9.0 (upstream `main` has moved to NiFi 2.10.0, whose NARs do not load on 2.9.0).
It is the upstream `v2.9.0` tag plus:

- upstream `f8a15fb` (makes the JUnit 4 suite run) and the Consumer Message Batch Size fix (upstream #142);
- upstream follow-ups #144, #145 (partitioned topics in `ConsumePulsarRecord`), #147 (no exception when a
  batch opens no record set), #149 (attribute docs), #150 (async acknowledgement Future leak) and
  #155 (`PublisherLease` waited on none of the sends beyond the first 100);
- the fix for upstream #156 (merged upstream as #158): `PublisherPool` now really pools producers per topic
  and closes every producer when the processor stops (`PublishPulsarRecord` returns its lease after each
  FlowFile);
- upstream follow-ups #159 (the previous pool is closed when the processor is rescheduled), #161 (`msg.key`
  attribute fallback — see the behaviour note above), #162 (bounded publish batch per trigger), #163
  (`ConsumePulsarRecord` no longer strands a FlowFile when parse-failure routing cannot write), #164 and #165
  (no empty FlowFiles or stray demarcators in async mode; new *Consumer Cache Size* property);
- the fix for upstream #167 (merged upstream as #169) and its follow-up #170: `ConsumePulsar` and
  `ConsumePulsarRecord` acknowledge a message only once the FlowFile carrying it has been committed, and
  never on a path that rolls the session back, so a write error makes the broker redeliver the batch
  instead of losing it;
- upstream #171 and #172 (#34 phases 1 and 2): the publishers validate content against the topic's schema
  and encode records with it — see [Publishing to topics that have a schema](#publishing-to-topics-that-have-a-schema)
  for the behaviour change — and #176 (`PublishPulsarRecord` keeps the records of a FlowFile in order);
- the fix for upstream #174, proposed upstream as #179: `ConsumePulsarRecord` starts a new record set when
  the record schema changes, so an inferred schema no longer drops the fields that the first message of a
  batch happens to lack — see [Record sets and the record schema](#record-sets-and-the-record-schema).

The Testcontainers integration tests that upstream added with these fixes (#152, #159, #160, #161, #166, #171, #172, #176) are
not carried on this build line because they need a Docker daemon.

The version follows the `<nifi.version>[.<revision>]` scheme of [VERSIONING.md](VERSIONING.md) with a
`-batchfix.N` qualifier so the artifacts cannot be confused with the upstream `2.9.0` release. Both NARs
must always be installed with the **same** version: `nifi-pulsar-nar` declares
`nifi-pulsar-client-service-nar` as its parent NAR.

The attribute contract is documented in [Consumer FlowFile attributes](#consumer-flowfile-attributes) above.

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
