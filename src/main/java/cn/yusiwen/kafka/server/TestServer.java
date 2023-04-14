package cn.yusiwen.kafka.server;

import org.apache.commons.cli.*;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestServer {

    public static final String MSG = "{\"status\": \"OK\", \"message\": \"\"}";

    private AtomicInteger count = new AtomicInteger(0);

    private DisposableServer disposableServer;

    public void start(int port) throws IOException {
        HttpServer httpServer = HttpServer.create().port(port)
                .route(routes -> routes.post("/receive", (req, res) -> {
                    int c = count.incrementAndGet();
                    System.out.println("Received packages count: " + c);
                    return res.sendString(Mono.just(MSG));
                }));
        httpServer.warmup().block();
        disposableServer = httpServer.bindNow();
        System.out.println("Listening on " + port);
        disposableServer.onDispose().block();
    }

    @PreDestroy
    public void destroy() {
        if (disposableServer != null) {
            disposableServer.disposeNow();
        }
    }

    public static void main(String[] args) {
        int port = 3000;

        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(Option.builder("PORT").option("p").longOpt("port")
                .desc("listen port")
                .hasArg().required(false)
                .build());

        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("port")) {
                port = Integer.parseInt(line.getOptionValue("port"));
            }
        } catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TestSender", options);
            System.exit(1);
        }

        TestServer testServer = new TestServer();
        try {
            testServer.start(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
