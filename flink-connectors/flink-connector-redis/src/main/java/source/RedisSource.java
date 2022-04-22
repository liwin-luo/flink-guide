package source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;

@Slf4j
public class RedisSource implements SourceFunction<String> {
    private boolean isRunning = true;
    private Jedis jedis = null;
    private final long sleepTime = 1;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        this.jedis = new Jedis("172.16.90.63", 6379);
        HashMap<String, String> kVMap = new HashMap<String, String>();
        while (isRunning) {
            try {
                kVMap.clear();
                String fruit = jedis.rpop("fruits");
                ctx.collect(fruit);
                Thread.sleep(sleepTime);
            } catch (JedisConnectionException e) {
                log.error(" redis 连接异常 ");
            } catch (Exception e) {
                log.error(" source 数据源异常", e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        while (jedis != null) {
            jedis.close();
        }
    }
}
