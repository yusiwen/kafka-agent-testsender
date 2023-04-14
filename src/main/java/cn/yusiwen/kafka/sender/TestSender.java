package cn.yusiwen.kafka.sender;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Siwen Yu (yusiwen@chinasofti.com)
 */
public class TestSender {

    private static final Logger log = LoggerFactory.getLogger(TestSender.class.getName());

    private static final String OPT_BROKER = "brokers";
    private static final String OPT_COUNT = "message-count";
    private static final String OPT_MSGFILE = "message-file";
    private static final String OPT_TOPIC = "topic";

    private final KafkaSender<String, String> sender;
    private final DateTimeFormatter dateFormat;

    public TestSender(String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);

        sender = KafkaSender.create(senderOptions);
        dateFormat = DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy");
    }

    public void sendMessages(String topic, int count, CountDownLatch latch, InputStream input) throws IOException {
        String msg = IOUtils.toString(input, StandardCharsets.UTF_8);
        sender.send(Flux.range(1, count)
                        .map(i -> SenderRecord.create(new ProducerRecord<>(topic, String.valueOf(i), msg), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();
                    Instant timestamp = Instant.ofEpochMilli(metadata.timestamp());
                    log.info(String.format("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                            r.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            dateFormat.format(ZonedDateTime.ofInstant(timestamp, ZoneId.systemDefault()))));
                    latch.countDown();
                });
    }

    public void close() {
        sender.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 1;
        InputStream input = null;
        String brokers = null;
        String topic = null;

        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(Option.builder("SIZE").option("c").longOpt(OPT_COUNT)
                .desc("message count")
                .hasArg().required(false)
                .build());
        options.addOption(Option.builder("BROKERS").option("b").longOpt(OPT_BROKER)
                .desc("kafka brokers")
                .hasArg().required(true)
                .build());
        options.addOption(Option.builder("TOPIC").option("t").longOpt(OPT_TOPIC)
                .desc("kafka topic")
                .hasArg().required(true)
                .build());
        options.addOption(Option.builder("FILES").option("f").longOpt(OPT_MSGFILE)
                .desc("message json file location")
                .hasArg().required(false)
                .build());

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            // validate that count has been set
            if (line.hasOption(OPT_COUNT)) {
                // print the value of block-size
                count = Integer.parseInt(line.getOptionValue(OPT_COUNT));
            }

            if (line.hasOption(OPT_MSGFILE)) {
                input = Files.newInputStream(Paths.get(line.getOptionValue(OPT_MSGFILE)));
            } else {
                input = TestSender.class.getClassLoader().getResourceAsStream("sample.json");
            }

            brokers = line.getOptionValue(OPT_BROKER);
            topic = line.getOptionValue(OPT_TOPIC);
        } catch (ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TestSender", options);
            System.exit(1);
        }

        CountDownLatch latch = new CountDownLatch(count);
        TestSender producer = new TestSender(brokers);
        producer.sendMessages(topic, count, latch, input);
        if (!latch.await(30, TimeUnit.SECONDS)) {
            log.warn("latch.wait() Interrupted");
        }
        producer.close();
    }
}