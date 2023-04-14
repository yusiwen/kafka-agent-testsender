package cn.yusiwen.kafka.sender;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class TestSenderTest {

    private static final Logger log = LoggerFactory.getLogger(TestSenderTest.class.getName());

    TestSender sender;
    InputStream input;

    String topic;

    @Setup
    public void setup() {
        sender = new TestSender("172.16.62.219:9092");
        input = TestSenderTest.class.getClassLoader().getResourceAsStream("sample.json");
        topic = "topic_yusiwen_tofsu";
    }

    @TearDown
    public void cleanup() {
        sender.close();
        log.info("FINISHED");
    }

    @Benchmark
    public void test() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(500000);

        sender.sendMessages(topic, 500000, latch, input);
        latch.await();
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(TestSenderTest.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
