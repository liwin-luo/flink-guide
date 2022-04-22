import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.RedisSource;

/**
 * @author zerocode
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<String> record = env.addSource(new RedisSource());
        record.print().setParallelism(1);

        env.execute("Flink Redis Source");
    }
}
