package bbs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
 * Bot padronizado da parte 2: cria canal, inscreve e publica em rajada.
 * Exemplo: USER_NAME=bot-1 java -jar bbs-bot.jar
 */
public final class Bot {
    private static final String USER = System.getenv().getOrDefault("USER_NAME", "bot-1");
    private static final String BROKER = System.getenv().getOrDefault("BROKER_URL", "tcp://broker:5555");
    private static final String PROXY = System.getenv().getOrDefault("PROXY_URL", "tcp://proxy:5558");
    private static final Set<String> SUBSCRIBED = new HashSet<>();
    private static final BlockingQueue<String> JOINS = new LinkedBlockingQueue<>();
    // Lamport do processo: a thread do REQ e a dos canais compartilham este contador.
    private static long clock = 0;

    private Bot() {}

    public static void main(String[] args) throws Exception {
        try (ZContext context = new ZContext()) {
            startFeed(context);
            ZMQ.Socket socket = connect(context);
            loginUntilOk(socket);
            publishLoop(socket);
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

    private static void publishLoop(ZMQ.Socket socket) throws Exception {
        while (true) {
            List<String> channels = list(socket);
            createIfFew(socket, channels);
            subscribeOne(channels);
            publishBurst(socket, channels);
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

    private static void createIfFew(ZMQ.Socket socket, List<String> channels) throws Exception {
        if (channels.size() >= 5) {
            return;
        }
        exchange(socket, Messages.createChannel(USER, freshName(channels)));
    }

    private static String freshName(List<String> channels) {
        int number = channels.size() + 1;
        String name = "Sala-" + number;
        while (channels.contains(name)) {
            number += 1;
            name = "Sala-" + number;
        }
        return name;
    }

    private static void subscribeOne(List<String> channels) {
        if (SUBSCRIBED.size() >= 3) {
            return;
        }
        List<String> open = new ArrayList<>();
        for (String name : channels) {
            if (!SUBSCRIBED.contains(name)) {
                open.add(name);
            }
        }
        if (open.isEmpty()) {
            return;
        }
        String channel = open.get(ThreadLocalRandom.current().nextInt(open.size()));
        SUBSCRIBED.add(channel);
        JOINS.offer(channel);
    }

    private static void publishBurst(ZMQ.Socket socket, List<String> channels) throws Exception {
        if (channels.isEmpty()) {
            return;
        }
        String channel = channels.get(ThreadLocalRandom.current().nextInt(channels.size()));
        sendTen(socket, channel);
    }

    private static void sendTen(ZMQ.Socket socket, String channel) throws Exception {
        for (int sent = 0; sent < 10; sent++) {
            waitBetween(sent);
            int value = ThreadLocalRandom.current().nextInt(0, 1_000_000);
            exchange(socket, Messages.publish(USER, channel, "mensagem " + value + " do " + USER));
        }
    }

    private static void waitBetween(int sent) throws InterruptedException {
        if (sent > 0) {
            Thread.sleep(1000);
        }
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
            observe(message.get("lc"));
            System.out.println(incomingLine(channel, message.get("text"), message.get("ts"), message.get("lc"), received));
        } catch (IOException | IllegalArgumentException ex) {
            System.out.println("Mensagem recebida ilegivel no canal " + topic + ": " + ex.getMessage());
        }
    }

    private static String incomingLine(String channel, Object text, Object sent, Object lc, long received) {
        return "Mensagem recebida: {canal=" + channel
            + ", mensagem=" + text
            + ", ts_envio=" + sent
            + ", lc=" + lc
            + ", ts_recebimento=" + received + "}";
    }

    private static Map<String, Object> exchange(ZMQ.Socket socket, Map<String, Object> request)
            throws IOException {
        stamp(request);
        System.out.println("Mensagem enviada: " + request);
        socket.send(Codec.pack(request));
        byte[] raw = socket.recv();
        Map<String, Object> reply = Codec.unpack(raw);
        observe(reply.get("lc"));
        System.out.println("Mensagem recebida: " + reply);
        return reply;
    }

    private static synchronized void stamp(Map<String, Object> message) {
        clock += 1;
        message.put("lc", clock);
    }

    private static synchronized void observe(Object value) {
        if (value instanceof Number number) {
            clock = Math.max(clock, number.longValue());
        }
    }
}
