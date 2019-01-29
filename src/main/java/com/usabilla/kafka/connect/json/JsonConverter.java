package com.usabilla.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonSchema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A KafkaConnect JsonConverter which is compatible with Tombstone messages
 * For the official support status please check https://issues.apache.org/jira/browse/KAFKA-3832
 * This Package should be deprecated as soon as Kafka supports this feature
 */
public class JsonConverter extends com.usabilla.kafka.connect.json.JsonConverterBase {
    private final static ObjectMapper mapper = new ObjectMapper();

    private static JsonNode schema;

    static {
        try {
            ClassLoader classLoader = JsonConverter.class.getClassLoader();
            File file = new File(classLoader.getResource("schema.json").getFile());

            schema = mapper.readTree(file);
            System.out.println("==============================================schema is:");
            System.out.println(schema.toString());
            System.out.println("==============================================schema end:");

        } catch (IOException e) {
            System.out.println("***************");
            e.printStackTrace();
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        System.out.println("==============================================222json is:");
        if (value == null) System.out.println("null byte message");
        System.out.println("topic is: " + topic);

        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        System.out.println("==============================================json is:");
        if (value == null) System.out.println("null byte message");
        System.out.println("byte msg: " + new String(value));
        System.out.println(jsonValue.toString());
        System.out.println("==============================================end");


        ObjectNode envelope = JsonNodeFactory.instance.objectNode();
        envelope.set("schema", schema);
        envelope.set("payload", jsonValue);
        jsonValue = envelope;

        return jsonToConnect(jsonValue);
    }
}
