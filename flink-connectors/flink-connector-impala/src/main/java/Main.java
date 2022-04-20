import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.Fruit;
import sink.ImpalaSource;

/**
 * Impala数据源
 *
 * @author zerocode
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<Fruit> record = env.addSource(new ImpalaSource());
        record.print().setParallelism(1);

        env.execute("Flink Impala Source");
    }
}
