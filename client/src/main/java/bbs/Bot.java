package bbs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Bot: login, canais, uma publicacao por volta e inscricao num canal sorteado.
 * Exemplo: USER_NAME=bot-1 java -jar bbs-bot.jar
 */
public final class Bot {
    private static final String USER = System.getenv().getOrDefault("USER_NAME", "bot-1");
    private static final String BROKER = System.getenv().getOrDefault("BROKER_URL", "tcp://broker:5555");
    private static final String PROXY = System.getenv().getOrDefault("PROXY_URL", "tcp://proxy:5558");
    private static final Set<String> SUBSCRIBED = new HashSet<>();
    private static final BlockingQueue<String> JOINS = new LinkedBlockingQueue<>();

    private Bot() {}

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            startFeed(context);
            ZMQ.Socket socket = connect(context);
            loginUntilOk(socket);
            keepChannels(socket);
        }
    }

    private static void startFeed(ZContext context) {
        Thread feed = new Thread(() -> listen(context), "canais-" + USER);
        feed.start();
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
            subscribeRandom(channels);
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

    private static void subscribeRandom(List<String> channels) {
        if (!SUBSCRIBED.isEmpty() || channels.isEmpty()) {
            return;
        }
        String channel = channels.get(ThreadLocalRandom.current().nextInt(channels.size()));
        SUBSCRIBED.add(channel);
        JOINS.offer(channel);
    }

    private static void listen(ZContext context) {
        ZMQ.Socket socket = openSub(context);
        while (true) {
            subscribePending(socket);
            readOne(socket);
        }
    }

    private static ZMQ.Socket openSub(ZContext context) {
        ZMQ.Socket socket = context.createSocket(SocketType.SUB);
        socket.setReceiveTimeOut(200);
        socket.connect(PROXY);
        return socket;
    }

    private static void subscribePending(ZMQ.Socket socket) {
        String channel = JOINS.poll();
        if (channel == null) {
            return;
        }
        socket.subscribe(channel.getBytes(StandardCharsets.UTF_8));
        System.out.println("Inscrito no canal: " + channel);
    }

    private static void readOne(ZMQ.Socket socket) {
        String topic = socket.recvStr();
        if (topic == null) {
            return;
        }
        byte[] body = socket.recv();
        if (body == null) {
            return;
        }
        printIncoming(topic, body);
    }

    private static void printIncoming(String topic, byte[] body) {
        long received = System.currentTimeMillis();
        try {
            Map<String, Object> message = Codec.unpack(body);
            String channel = String.valueOf(message.getOrDefault("channel", topic));
            System.out.println(incomingLine(channel, message.get("text"), message.get("ts"), received));
        } catch (IOException | IllegalArgumentException ex) {
            System.out.println("Mensagem recebida ilegivel no canal " + topic + ": " + ex.getMessage());
        }
    }

    private static String incomingLine(String channel, Object text, Object sent, long received) {
        return "Mensagem recebida: {canal=" + channel
            + ", mensagem=" + text
            + ", ts_envio=" + sent
            + ", ts_recebimento=" + received + "}";
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
