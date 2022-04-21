import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.kudu.batch.KuduRowInputFormat;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author EDZ
 */
public class Main {
    public static void main(String[] args) throws Exception {
        //本地环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        String KUDU_MASTERS = "172.16.90.65:7051,172.16.90.63:7051,172.16.90.61:7051";
        KuduReaderConfig readerConfig = KuduReaderConfig.Builder.setMasters(KUDU_MASTERS).build();
        KuduTableInfo tableInfo = KuduTableInfo.forTable("impala::default.fruit");
        KuduRowInputFormat inputFormat = new KuduRowInputFormat(readerConfig, tableInfo);
        //
        DataStream<Row> rowStream = env.createInput(inputFormat, TypeInformation.of(Row.class));
        //打印结果
        rowStream.print();

        env.execute("Flink Impala Source");
    }
}
