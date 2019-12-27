package source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * MySQLSource，自定义 Source 类
 */
public class MySQLSource extends AbstractSource implements Configurable, PollableSource {

    // 打印日志
    private static final Logger LOG = LoggerFactory.getLogger(MySQLSource.class);

    // sqlHelper
    private MySQLSourceHelper sqlSourceHelper;

    // 两次查询的时间间隔
    private int queryDelay;
    private static final int DEFAULT_QUERY_DELAY = 10000;

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        // 初始化
        sqlSourceHelper = new MySQLSourceHelper(context);
        queryDelay = context.getInteger("query.delay", DEFAULT_QUERY_DELAY);
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            // 存放 event 的集合
            List<Event> events = new ArrayList<>();
            // 存放 event 头集合
            HashMap<String, String> header = new HashMap<>();
            header.put("table", sqlSourceHelper.getTable());

            // 查询数据表
            List<List<Object>> result = sqlSourceHelper.executeQuery();
            // 如果有返回数据，则将数据封装为 event
            if (!result.isEmpty()) {
                List<String> allRows = sqlSourceHelper.getAllRows(result);
                Event event = null;
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setHeaders(header);
                    event.setBody(row.getBytes());
                    events.add(event);
                }
                // 将 event 写入 channel
                getChannelProcessor().processEventBatch(events);
                // 更新数据表中的 offset 信息，取最后一条数据的第一列（id 列）
                sqlSourceHelper.updateOffset2DB(new BigInteger(result.get(result.size()-1).get(0).toString()));
            }
            // 等待时长
            Thread.sleep(queryDelay);
            return Status.READY;
        } catch (InterruptedException e) {
            LOG.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping sql source {} ...", getName());
        try {
            sqlSourceHelper.close();
        } finally {
            super.stop();
        }
    }
}