package bbs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Bot: login, canais e uma publicacao por volta em geral.
 * Exemplo: USER_NAME=bot-1 java -jar bbs-bot.jar
 */
public final class Bot {
    private static final String USER = System.getenv().getOrDefault("USER_NAME", "bot-1");
    private static final String BROKER = System.getenv().getOrDefault("BROKER_URL", "tcp://broker:5555");

    private Bot() {}

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = connect(context);
            loginUntilOk(socket);
            keepChannels(socket);
        }
    }

    private static ZMQ.Socket connect(ZContext context) throws InterruptedException {
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.connect(BROKER);
        Thread.sleep(1000);
        return socket;
    }

    private static void loginUntilOk(ZMQ.Socket socket) throws Exception {
        while (true) {
            Map<String, Object> reply = exchange(socket, Messages.login(USER));
            if ("login_ok".equals(reply.get("op"))) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    private static void keepChannels(ZMQ.Socket socket) throws Exception {
        while (true) {
            List<String> channels = list(socket);
            ensure(socket, channels, "geral");
            ensure(socket, channels, "sala-" + USER);
            publishGeral(socket);
            Thread.sleep(3000);
        }
    }

    private static List<String> list(ZMQ.Socket socket) throws Exception {
        Map<String, Object> reply = exchange(socket, Messages.listChannels(USER));
        Object value = reply.get("channels");
        if (value instanceof List<?> names) {
            return names.stream().map(String::valueOf).toList();
        }
        throw new IllegalArgumentException("channels invalido: " + value);
    }

    private static void publishGeral(ZMQ.Socket socket) throws Exception {
        exchange(socket, Messages.publish(USER, "geral", "mensagem de " + USER));
    }

    private static void ensure(ZMQ.Socket socket, List<String> known, String channel) throws Exception {
        if (known.contains(channel)) {
            return;
        }
        exchange(socket, Messages.createChannel(USER, channel));
    }

    private static Map<String, Object> exchange(ZMQ.Socket socket, Map<String, Object> request)
            throws IOException {
        System.out.println("Mensagem enviada: " + request);
        socket.send(Codec.pack(request));
        byte[] raw = socket.recv();
        Map<String, Object> reply = Codec.unpack(raw);
        System.out.println("Mensagem recebida: " + reply);
        return reply;
    }
}
