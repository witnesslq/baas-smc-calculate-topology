//package com.ai.baas.smc.calculate.topology.core.util;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.Map.Entry;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.ai.baas.storm.util.BaseConstants;
//import com.ai.baas.storm.util.HBaseProxy;
//import com.ai.opt.base.exception.SystemException;
//import com.ai.opt.sdk.constants.SDKConstants;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//
//public class HbaseClient {
//
//	private static Connection connection;
//	private static Configuration configuration;
//    private static Logger logger = LoggerFactory.getLogger(HbaseClient.class);
//
//    public static final String FIELD_SPLIT = new String(new char[] { (char) 1 });
//
//
//    private static Properties prop = new Properties();
//
//
//    private static HbaseClient hbaseClient;
//
//  
//    
//    public static void loadResource(Map<String,String> config){
//		 configuration = HBaseConfiguration.create();
//		String hbaseSite = config.get(BaseConstants.HBASE_PARAM);
//		try {
//			if(StringUtils.isBlank(hbaseSite)){
//				throw new Exception("输入参数中没有配置hbase.site属性信息!");
//			}
//			JsonParser jsonParser = new JsonParser();
//			JsonObject jsonObject = (JsonObject)jsonParser.parse(hbaseSite);
//			for(Entry<String, JsonElement> entry:jsonObject.entrySet()){
//				configuration.set(entry.getKey(), entry.getValue().getAsString());
//			}
//			connection = ConnectionFactory.createConnection(configuration);
//		} catch (Exception e) {
//			logger.error("error", e);
//		}
//	}
//    
//    public Configuration getConfiguration()
//    {
//    	return configuration;
//    }
//    
//
//    public Connection getConnection() {
//        return connection;
//    }
//
//  
//    
//
//    /*
//     * 创建表
//     * 
//     * @tableName 表名
//     * 
//     * @family 列族列表
//     */
//    public static void creatTable(String tableName, String[] family) throws Exception {
//        HBaseAdmin admin = new HBaseAdmin(configuration);
//        HTableDescriptor desc = new HTableDescriptor(tableName);
//        for (int i = 0; i < family.length; i++) {
//            desc.addFamily(new HColumnDescriptor(family[i]));
//        }
//        if (admin.tableExists(tableName)) {
//            System.out.println("table Exists!");
//            System.exit(0);
//        } else {
//            admin.createTable(desc);
//            System.out.println("create table Success!");
//        }
//    }
//
//    /*
//     * 根据rwokey查询
//     * 
//     * @rowKey rowKey
//     * 
//     * @tableName 表名
//     */
//    private static Result getResult(String tableName, String rowKey) throws IOException {
//        Get get = new Get(Bytes.toBytes(rowKey));
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));// 获取表
//        Result result = table.get(get);
//        for (KeyValue kv : result.list()) {
//            System.out.println("family:" + Bytes.toString(kv.getFamily()));
//            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//            System.out.println("value:" + Bytes.toString(kv.getValue()));
//            System.out.println("Timestamp:" + kv.getTimestamp());
//            System.out.println("-------------------------------------------");
//        }
//        return result;
//    }
//
//    /*
//     * 遍历查询hbase表
//     * 
//     * @tableName 表名
//     */
//    private static void getResultScann(String tableName) throws IOException {
//        Scan scan = new Scan();
//        ResultScanner rs = null;
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        try {
//            rs = table.getScanner(scan);
//            for (Result r : rs) {
//                for (KeyValue kv : r.list()) {
//                    System.out.println("row:" + Bytes.toString(kv.getRow()));
//                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
//                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//                    System.out.println("value:" + Bytes.toString(kv.getValue()));
//                    System.out.println("timestamp:" + kv.getTimestamp());
//                    System.out.println("-------------------------------------------");
//                }
//            }
//        } finally {
//            rs.close();
//        }
//    }
//
//    /*
//     * 遍历查询hbase表
//     * 
//     * @tableName 表名
//     */
//    private static void getResultScann(String tableName, String start_rowkey, String stop_rowkey)
//            throws IOException {
//        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes(start_rowkey));
//        scan.setStopRow(Bytes.toBytes(stop_rowkey));
//        ResultScanner rs = null;
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        try {
//            rs = table.getScanner(scan);
//            for (Result r : rs) {
//                for (KeyValue kv : r.list()) {
//                    System.out.println("row:" + Bytes.toString(kv.getRow()));
//                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
//                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//                    System.out.println("value:" + Bytes.toString(kv.getValue()));
//                    System.out.println("timestamp:" + kv.getTimestamp());
//                    System.out.println("-------------------------------------------");
//                }
//            }
//        } finally {
//            rs.close();
//        }
//    }
//
//    /*
//     * 查询表中的某一列
//     * 
//     * @tableName 表名
//     * 
//     * @rowKey rowKey
//     */
//    private static void getResultByColumn(String tableName, String rowKey, String familyName,
//            String columnName) throws IOException {
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
//        Result result = table.get(get);
//        for (KeyValue kv : result.list()) {
//            System.out.println("family:" + Bytes.toString(kv.getFamily()));
//            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//            System.out.println("value:" + Bytes.toString(kv.getValue()));
//            System.out.println("Timestamp:" + kv.getTimestamp());
//            System.out.println("-------------------------------------------");
//        }
//    }
//
//    /*
//     * 更新表中的某一列
//     * 
//     * @tableName 表名
//     * 
//     * @rowKey rowKey
//     * 
//     * @familyName 列族名
//     * 
//     * @columnName 列名
//     * 
//     * @value 更新后的值
//     */
//    private static void updateTable(String tableName, String rowKey, String familyName,
//            String columnName, String value) throws IOException {
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
//        table.put(put);
//        System.out.println("update table Success!");
//    }
//
//    /*
//     * 查询某列数据的多个版本
//     * 
//     * @tableName 表名
//     * 
//     * @rowKey rowKey
//     * 
//     * @familyName 列族名
//     * 
//     * @columnName 列名
//     */
//    private static void getResultByVersion(String tableName, String rowKey, String familyName,
//            String columnName) throws IOException {
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
//        get.setMaxVersions(5);
//        Result result = table.get(get);
//        for (KeyValue kv : result.list()) {
//            System.out.println("family:" + Bytes.toString(kv.getFamily()));
//            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//            System.out.println("value:" + Bytes.toString(kv.getValue()));
//            System.out.println("Timestamp:" + kv.getTimestamp());
//            System.out.println("-------------------------------------------");
//        }
//        /*
//         * List<?> results = table.get(get).list(); Iterator<?> it = results.iterator(); while
//         * (it.hasNext()) { System.out.println(it.next().toString()); }
//         */
//    }
//
//    /*
//     * 删除指定的列
//     * 
//     * @tableName 表名
//     * 
//     * @rowKey rowKey
//     * 
//     * @familyName 列族名
//     * 
//     * @columnName 列名
//     */
//    private static void deleteColumn(String tableName, String rowKey, String falilyName,
//            String columnName) throws IOException {
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
//        deleteColumn.deleteColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
//        table.delete(deleteColumn);
//        System.out.println(falilyName + ":" + columnName + "is deleted!");
//    }
//
//    /*
//     * 删除指定的列
//     * 
//     * @tableName 表名
//     * 
//     * @rowKey rowKey
//     */
//    private static void deleteAllColumn(String tableName, String rowKey) throws IOException {
//        HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
//        table.delete(deleteAll);
//        System.out.println("all columns are deleted!");
//    }
//
//    /*
//     * 删除表
//     * 
//     * @tableName 表名
//     */
//    private static void deleteTable(String tableName) throws IOException {
//        HBaseAdmin admin = new HBaseAdmin(configuration);
//        admin.disableTable(tableName);
//        admin.deleteTable(tableName);
//        System.out.println(tableName + "is deleted!");
//    }
//    
//    
//    // 添加一条数据
//    public static void addRow(String tableName, String row,
//            String columnFamily, String[] column, String[] value) throws Exception {
//        HTable table = new HTable(configuration, tableName);
//        Put put = new Put(Bytes.toBytes(row));// 指定行
//        // 参数分别:列族、列、值
//        for(int i=0;i<column.length;i++)
//        {
//        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column[i]),
//                Bytes.toBytes(value[i]));
//        }
//        table.put(put);
//    }
//    
//    
//    public static void addRowByMap(String tableName,String row,String columnFamily,
//    		Map<String,String> map) throws Exception{
//    	  HTable table = new HTable(configuration, tableName);
//          Put put = new Put(Bytes.toBytes(row));// 指定行
//          // 参数分别:列族、列、值
//          for (String key : map.keySet()) {
//        	   put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key),
//                       Bytes.toBytes(map.get(key)));
//        	  }
//         
//          table.put(put);
//    }
//    
//    
//   
//    public void getRTM(String tableName,String tenantId,String obj) throws IOException
//    {
//    	 HTable table = new HTable(configuration, Bytes.toBytes(tableName));
//    	 
//    	  Get get = new Get(Bytes.toBytes("170901818362016040811111123456"));
//    	  Result result = table.get(get);
//    	  
//    	  System.out.println(result.list());
//    	 
//    }
//
//}
