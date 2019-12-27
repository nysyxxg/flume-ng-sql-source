package source;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQLSourceHelper，JDBC 工具类，主要是读取数据表和更新读取记录
 *
 */
public class MySQLSourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLSourceHelper.class);

    // 开始 id
    private String startFrom;
    private static final String DEFAULT_START_VALUE = "0";

    // 表名
    private String table;
    // 用户传入的查询的列
    private String columnsToSelect;
    private static final String DEFAULT_Columns_To_Select = "*";

    private static String dbUrl, dbUser, dbPassword, dbDriver;
    private static Connection conn = null;
    private static PreparedStatement ps = null;

    // 获取 JDBC 连接
    private static Connection getConnection() {
        try {
            Class.forName(dbDriver);
            return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 构造方法
    MySQLSourceHelper(Context context) {
        // 有默认值参数：获取 flume 任务配置文件中的参数，读不到的采用默认值
        this.startFrom = context.getString("start.from", DEFAULT_START_VALUE);
        this.columnsToSelect = context.getString("columns.to.select", DEFAULT_Columns_To_Select);

        // 无默认值参数：获取 flume 任务配置文件中的参数
        this.table = context.getString("table");

        dbUrl = context.getString("db.url");
        dbUser = context.getString("db.user");
        dbPassword = context.getString("db.password");
        dbDriver = context.getString("db.driver");
        conn = getConnection();
    }

    // 构建 sql 语句，以 id 作为 offset
    private String buildQuery() {
        StringBuilder execSql = new StringBuilder("select " + columnsToSelect + " from " + table);
        return execSql.append(" where id ").append("> ").append(getStatusDBIndex(startFrom)).toString();
    }

    // 执行查询
    List<List<Object>> executeQuery() {
        try {
            // 每次执行查询时都要重新生成 sql，因为 id 不同
            String customQuery = buildQuery();
            // 存放结果的集合
            List<List<Object>> results = new ArrayList<>();

            ps = conn.prepareStatement(customQuery);
            ResultSet result = ps.executeQuery(customQuery);
            while (result.next()) {
                // 存放一条数据的集合（多个列）
                List<Object> row = new ArrayList<>();
                // 将返回结果放入集合
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }
                results.add(row);
            }
            LOG.info("execSql:" + customQuery + "\tresultSize:" + results.size());
            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());
            // 重新连接
            conn = getConnection();
        }
        return null;
    }

    // 将结果集转化为字符串，每一条数据是一个 list 集合，将每一个小的 list 集合转化为字符串
    List<String> getAllRows(List<List<Object>> queryResult) {
        List<String> allRows = new ArrayList<>();
        StringBuilder row = new StringBuilder();
        for (List<Object> rawRow : queryResult) {
            for (Object aRawRow : rawRow) {
                if (aRawRow == null) {
                    row.append(",");
                } else {
                    row.append(aRawRow.toString()).append(",");
                }
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }
        return allRows;
    }

    // 更新 offset 元数据状态，每次返回结果集后调用。必须记录每次查询的 offset 值，为程序中断续跑数据时使用，以 id 为 offset
    void updateOffset2DB(BigInteger size) {
        try {
            // 以 source_tab 做为 KEY，如果不存在则插入，存在则更新（每个源表对应一条记录）
            String sql = "insert into flume_meta VALUES('" + table + "','" + size + "') on DUPLICATE key update current_index='" + size + "'";
            LOG.info("updateStatus Sql:" + sql);
            ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 从 flume_meta 表中查询出当前的 id 是多少
    private BigInteger getStatusDBIndex(String startFrom) {
        BigInteger dbIndex = new BigInteger(startFrom);
        try {
            ps = conn.prepareStatement("select current_index from flume_meta where source_tab='" + table + "'");
            ResultSet result = ps.executeQuery();
            if (result.next()) {
                String id = result.getString(1);
                if (id != null) {
                    dbIndex = new BigInteger(id);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初传入的值
        return dbIndex;
    }

    // 关闭相关资源
    void close() {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getTable() {
        return table;
    }
}