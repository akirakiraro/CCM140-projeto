package bbs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/** MessagePack de mapas string -> escalar ou lista de strings. */
final class Codec {
    private Codec() {}

    static byte[] pack(Map<String, Object> message) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(message.size());
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            packer.packString(entry.getKey());
            packValue(packer, entry.getValue());
        }
        packer.close();
        return packer.toByteArray();
    }

    static Map<String, Object> unpack(byte[] raw) throws IOException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(raw);
        int size = unpacker.unpackMapHeader();
        Map<String, Object> message = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            String key = unpacker.unpackString();
            message.put(key, toJava(unpacker.unpackValue()));
        }
        unpacker.close();
        return message;
    }

    private static void packValue(MessageBufferPacker packer, Object value) throws IOException {
        if (value instanceof String text) {
            packer.packString(text);
            return;
        }
        if (value instanceof Long || value instanceof Integer) {
            packer.packLong(((Number) value).longValue());
            return;
        }
        if (value instanceof List<?> list) {
            packList(packer, list);
            return;
        }
        throw new IllegalArgumentException("valor nao serializavel: " + value);
    }

    private static void packList(MessageBufferPacker packer, List<?> list) throws IOException {
        packer.packArrayHeader(list.size());
        for (Object item : list) {
            packValue(packer, item);
        }
    }

    private static Object toJava(Value value) {
        ValueType type = value.getValueType();
        if (type == ValueType.STRING) {
            return value.asStringValue().asString();
        }
        if (type == ValueType.INTEGER) {
            return value.asIntegerValue().asLong();
        }
        if (type == ValueType.ARRAY) {
            return toList(value);
        }
        throw new IllegalArgumentException("tipo msgpack inesperado: " + value);
    }

    private static List<String> toList(Value value) {
        List<String> items = new ArrayList<>();
        for (Value item : value.asArrayValue()) {
            if (!item.isStringValue()) {
                throw new IllegalArgumentException("item de lista invalido: " + item);
            }
            items.add(item.asStringValue().asString());
        }
        return items;
    }
}
