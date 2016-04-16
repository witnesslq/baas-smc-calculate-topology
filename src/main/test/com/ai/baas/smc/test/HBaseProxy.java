package com.ai.baas.smc.test;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseProxy {

	private static Connection connection;

	static {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "49181");
		configuration.set("hbase.zookeeper.quorum", "10.1.130.84,10.1.130.85,10.1.236.122");
		configuration.set("hbase.master", "10.1.130.84");
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void main(String[] args) throws IOException {
		Scan scan = new Scan();
		Table table = connection.getTable(TableName.valueOf("RTM_OUTPUT_DETAIL_201604"));
		 ResultScanner rs = null;

		 rs = table.getScanner(scan);
	      table.close();
		for (Result r : rs) {// 按行去遍历
			String line="";
	        for (KeyValue kv : r.raw()) {// 遍历每一行的各列
	        	if(Bytes.toString(kv.getQualifier()).equals("record"))
	        	{
	        		line=Bytes.toString(kv.getValue());
	        	}
	          
	          System.out.println(line);
	         
	        }

	      }


	}

}
