import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * 单词统计
 *
 * @author zerocode
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //本地环境
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        //获取用户目录
        String userDir = System.getProperty("user.dir");
        String baseDir = userDir + File.separator + "flink-helloworld" + File.separator;
        //输入文件路径
        String filePath = baseDir + "in.txt";

        //从指定路径的文件读取数据
        DataSet<String> text = env.readTextFile(filePath);

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);
        //1.打印统计结果
        counts.print();
//        //2.输出到文件
//        String outPutPath = baseDir + "out.txt";
//        counts.writeAsText(outPutPath);
    }

    /**
     * 分组方法
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.split(" ");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
