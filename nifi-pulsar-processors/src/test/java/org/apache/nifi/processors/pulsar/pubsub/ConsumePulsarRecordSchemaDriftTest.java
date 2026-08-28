/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarMessage;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Before;
import org.junit.Test;

/**
 * The schema a record set is written with must describe every message in the set, not just the first one.
 * <p>
 * ConsumePulsarRecord used to create the set's writer from the schema of its first message. With a reader
 * that infers the schema from the payload - the default of JsonTreeReader - every later message of the set
 * was written through that schema, so any field the first message did not have was silently dropped, and any
 * field it had that a later message lacked came out as null. Nothing was logged and nothing was routed to
 * parse_failure. These tests run the real JsonTreeReader and JsonRecordSetWriter, because the loss only shows
 * with a reader whose schema differs from message to message.
 */
public class ConsumePulsarRecordSchemaDriftTest extends AbstractPulsarProcessorTest<GenericRecord> {

    private static final String TOPIC = "persistent://public/default/telemetry";

    private static final String MESSAGE_1 = "{\"id\":1,\"a\":\"x\",\"nested\":{\"p\":1},\"tags\":[{\"k\":\"t1\"}],\"v\":1}";
    private static final String MESSAGE_2 = "{\"id\":2,\"a\":\"y\",\"extra\":\"only-in-2\",\"nested\":{\"p\":2,\"q\":\"only-in-2\"},"
            + "\"tags\":[{\"k\":\"t2\",\"m\":true}],\"v\":\"text\"}";
    private static final String MESSAGE_3 = "{\"id\":3,\"b\":true}";

    private JsonTreeReader reader;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);
        addPulsarClientService();

        reader = new JsonTreeReader();
        runner.addControllerService("record-reader", reader);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty(ConsumePulsarRecord.RECORD_READER, "record-reader");
        runner.setProperty(ConsumePulsarRecord.RECORD_WRITER, "record-writer");
        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, TOPIC);
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "nifi-subscription");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_TYPE, "Shared");
        runner.setProperty(AbstractPulsarConsumerProcessor.ASYNC_ENABLED, "false");
        runner.setProperty(AbstractPulsarConsumerProcessor.CONSUMER_BATCH_SIZE, "10");
        runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
    }

    /**
     * The case from the issue: three messages, each with fields the others do not have. Every field has to
     * survive, in one record set. Before the fix the output was
     * {@code [{"id":1,"a":"x","nested":{"p":1},...},{"id":2,"a":"y","nested":{"p":2},...},{"id":3,"a":null,...}]}:
     * {@code extra}, {@code nested.q}, {@code tags[].m} and {@code b} were gone.
     */
    @Test
    public void fieldsMissingFromTheFirstMessageAreKept() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_2, MESSAGE_3));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 0);
        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));

        final String content = new String(flowFile.toByteArray(), UTF_8);
        assertTrue("a top-level field the first message lacks was dropped: " + content, content.contains("\"extra\":\"only-in-2\""));
        assertTrue("a nested field the first message lacks was dropped: " + content, content.contains("\"q\":\"only-in-2\""));
        assertTrue("an array element field the first message lacks was dropped: " + content, content.contains("\"m\":true"));
        assertTrue("a field only the last message has was dropped: " + content, content.contains("\"b\":true"));
        // a field whose type differs between messages keeps each message's own value
        assertTrue(content, content.contains("\"v\":1"));
        assertTrue(content, content.contains("\"v\":\"text\""));
    }

    /** Messages with the same shape keep sharing one record set, exactly as before. */
    @Test
    public void messagesWithTheSameSchemaShareOneRecordSet() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_1.replace("\"id\":1", "\"id\":2"), MESSAGE_1.replace("\"id\":1", "\"id\":3")));

        runner.run(1, true);

        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));
        assertEquals("3", flowFile.getAttribute(ConsumePulsarRecord.MSG_COUNT));
    }

    /**
     * A change in the mapped attributes still starts a new record set, and each set is written with the
     * schema of its own messages only.
     */
    @Test
    public void eachRecordSetGetsTheSchemaOfItsOwnMessages() {
        runner.setProperty(AbstractPulsarConsumerProcessor.MAPPED_FLOWFILE_ATTRIBUTES, "kind");
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(
                message(MESSAGE_1, "kind", "first"),
                message(MESSAGE_2, "kind", "first"),
                message(MESSAGE_3, "kind", "second")));

        runner.run(1, true);

        final List<MockFlowFile> flowFiles = successFlowFiles(2);
        final String first = new String(flowFiles.get(0).toByteArray(), UTF_8);
        final String second = new String(flowFiles.get(1).toByteArray(), UTF_8);

        assertEquals("first", flowFiles.get(0).getAttribute("kind"));
        assertEquals("2", flowFiles.get(0).getAttribute("record.count"));
        assertTrue(first, first.contains("\"extra\":\"only-in-2\""));
        assertFalse("the first set must not carry fields of the second set's messages: " + first, first.contains("\"b\""));

        assertEquals("second", flowFiles.get(1).getAttribute("kind"));
        assertEquals("1", flowFiles.get(1).getAttribute("record.count"));
        assertTrue(second, second.contains("\"b\":true"));
        assertFalse("the second set must not carry fields of the first set's messages: " + second, second.contains("\"extra\""));
    }

    /** An unparseable message in the middle of the set goes to parse_failure without affecting the schema. */
    @Test
    public void anUnparseableMessageDoesNotDisturbTheRecordSet() {
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, "this is not json", MESSAGE_3));

        runner.run(1, true);

        runner.assertTransferCount(ConsumePulsarRecord.REL_PARSE_FAILURE, 1);
        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("2", flowFile.getAttribute("record.count"));
        final String content = new String(flowFile.toByteArray(), UTF_8);
        assertTrue(content, content.contains("\"a\":\"x\""));
        assertTrue(content, content.contains("\"b\":true"));
        assertEquals("this is not json", new String(runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE).get(0).toByteArray(), UTF_8));
    }

    /**
     * With an explicit schema nothing changes: every message resolves to the same schema, the set is written
     * with it, and fields outside it are dropped by design.
     */
    @Test
    public void anExplicitSchemaIsAppliedUnchanged() {
        runner.setProperty(reader, "Schema Access Strategy", "schema-text-property");
        runner.setProperty(reader, "Schema Text", "{\"type\":\"record\",\"name\":\"event\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"a\",\"type\":[\"null\",\"string\"]}]}");
        runner.enableControllerService(reader);
        mockClientService.setMockMessageQueue(messages(MESSAGE_1, MESSAGE_2, MESSAGE_3));

        runner.run(1, true);

        final MockFlowFile flowFile = successFlowFiles(1).get(0);
        assertEquals("3", flowFile.getAttribute("record.count"));
        final String content = new String(flowFile.toByteArray(), UTF_8);
        // every record resolves to the explicit schema, so the writer emits each payload's own fields verbatim
        assertEquals("[{\"id\":1,\"a\":\"x\"},{\"id\":2,\"a\":\"y\"},{\"id\":3}]", content);
    }

    private List<MockFlowFile> successFlowFiles(final int expected) {
        runner.assertTransferCount(ConsumePulsarRecord.REL_SUCCESS, expected);
        return runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
    }

    private static List<Message<GenericRecord>> messages(final String... payloads) {
        final List<Message<GenericRecord>> msgs = new ArrayList<>();
        for (final String payload : payloads) {
            msgs.add(message(payload, null, null));
        }
        return msgs;
    }

    @SuppressWarnings("unchecked")
    private static List<Message<GenericRecord>> messages(final Message<GenericRecord>... msgs) {
        final List<Message<GenericRecord>> list = new ArrayList<>();
        Collections.addAll(list, msgs);
        return list;
    }

    private static int nextId = 1;

    private static Message<GenericRecord> message(final String payload, final String propertyName, final String propertyValue) {
        final Map<String, String> properties = propertyName == null ? null : Collections.singletonMap(propertyName, propertyValue);
        return new MockPulsarMessage<GenericRecord>(TOPIC, payload.getBytes(UTF_8), "1234:" + (nextId++) + ":0", properties, null);
    }
}
