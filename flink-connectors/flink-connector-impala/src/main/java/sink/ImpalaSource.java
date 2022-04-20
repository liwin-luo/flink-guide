package sink;


import com.cloudera.impala.jdbc41.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import pojo.Fruit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ImpalaSource extends RichSourceFunction<Fruit> {

    private PreparedStatement ps = null;
    private Connection connection = null;
    String driver = "com.cloudera.impala.jdbc41.Driver";
    String url = "jdbc:impala://172.16.90.63:21050/default;auth=noSasl";
    String username = "";
    String password = "";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    public List<Fruit> query() throws SQLException {
        List<Fruit> fruits = new ArrayList<>();

        String sql = "SELECT * FROM fruit limit 10;";
        PreparedStatement ps = getConnection().prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Fruit fruit = new Fruit(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("type").trim(),
                    resultSet.getDouble("price"));
            fruits.add(fruit);
        }

        return fruits;
    }

    /**
     * DataStream 调用一次 run()方法执行查询并处理结果集
     * org.apache.flink.streaming.api.functions.source.SourceFunction#run
     */
    @Override
    public void run(SourceContext<Fruit> ctx) throws Exception {
        List<Fruit> fruits = query();
        fruits.forEach(fruit -> {
            ctx.collect(fruit);
        });
    }

    /**
     * 取消一个job时
     * org.apache.flink.streaming.api.functions.source.SourceFunction#cancel()
     */
    @Override
    public void cancel() {
    }

    /**
     * 关闭数据库连接
     * org.apache.flink.api.common.functions.AbstractRichFunction#close()
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    public Connection getConnection() {
        try {
            //加载驱动
            Class.forName(driver);
            //创建连接
            DataSource ds = new com.cloudera.impala.jdbc41.DataSource();
            ds.setURL(url);

            return ds.getConnection();
        } catch (Exception e) {
            log.error(" get connection occur exception, msg: {} " + e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }
}
