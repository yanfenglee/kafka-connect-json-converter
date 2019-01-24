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
import org.apache.kafka.connect.json.JsonConverterBase;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A KafkaConnect JsonConverter which is compatible with Tombstone messages
 * For the official support status please check https://issues.apache.org/jira/browse/KAFKA-3832
 * This Package should be deprecated as soon as Kafka supports this feature
 */
public class JsonConverter extends JsonConverterBase {
    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if ((jsonValue == null || !jsonValue.isObject() || jsonValue.size() != 2 || !jsonValue.has("schema") || !jsonValue.has("payload")))
            throw new DataException("JsonConverter with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields." +
                    " If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.");

        // The deserialized data should either be an envelope object containing the schema and the payload or the schema
        // was stripped during serialization and we need to fill in an all-encompassing schema.

        String schema_str = "{\n" +
                "    \"type\":\"record\",\n" +
                "    \"name\":\"logs\",\n" +
                "    \"fields\":[\n" +
                "        {\"name\":\"uid\",\"type\":\"string\"},\n" +
                "        {\"name\":\"thread\",\"type\":\"string\"},\n" +
                "        {\"name\":\"level\",\"type\":\"string\"}\n" +
                "    ]\n" +
                "}";



        JsonNode schema = null;
        try {
            schema = mapper.readTree(schema_str);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ObjectNode envelope = JsonNodeFactory.instance.objectNode();
        envelope.set("schema", schema);
        envelope.set("payload", jsonValue);
        jsonValue = envelope;

        return jsonToConnect(jsonValue);
    }
}
