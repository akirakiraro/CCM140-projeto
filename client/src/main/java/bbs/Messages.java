package bbs;

import java.util.LinkedHashMap;
import java.util.Map;

/** Pedidos do bot. O ts e o instante do envio, em milissegundos. */
final class Messages {
    private Messages() {}

    static Map<String, Object> login(String user) {
        Map<String, Object> message = base("login");
        message.put("user", user);
        return message;
    }

    static Map<String, Object> createChannel(String user, String channel) {
        Map<String, Object> message = base("create_channel");
        message.put("user", user);
        message.put("channel", channel);
        return message;
    }

    static Map<String, Object> listChannels(String user) {
        Map<String, Object> message = base("list_channels");
        message.put("user", user);
        return message;
    }

    private static Map<String, Object> base(String op) {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("op", op);
        message.put("ts", System.currentTimeMillis());
        return message;
    }
}
