package com.ai.baas.smc.calculate.topology.core.spout;

import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.ai.baas.storm.util.HBaseProxy;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CalSpout extends BaseRichSpout{

	 private SpoutOutputCollector collector;
	 
	 private static Connection connection;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		   HBaseProxy.loadResource(conf);
		   connection=HBaseProxy.getConnection();
		   this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Scan scan = new Scan();
		try
		{
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
	          
	        	collector.emit(new Values(line));
	         
	        }
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("source"));
	}

}
