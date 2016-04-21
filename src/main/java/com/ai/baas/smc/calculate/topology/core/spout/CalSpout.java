package com.ai.baas.smc.calculate.topology.core.spout;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.calculate.topology.core.bo.FinishListVo;
import com.ai.baas.smc.calculate.topology.core.flow.CalFeesFlow;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CalSpout extends BaseRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(CalSpout.class);

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
		logger.error("spout开始..........");
		ICacheClient cacheStatsTimes = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
		String finishlist = cacheStatsTimes.get(SmcCacheConstant.Cache.finishKey);
		logger.error("读取缓存结束..........");
		List<FinishListVo> voList = JSON.parseArray(finishlist, FinishListVo.class);
		for (FinishListVo vo : voList) {
		String tenantId=vo.getTenantId();
		String batchNo=vo.getBatchNo();
		String billTimeSn=vo.getBillTimeSn();
		Scan scan = new Scan();
		
		try
		{
		Table table = connection.getTable(TableName.valueOf("RTM_OUTPUT_DETAIL_"+billTimeSn));
		 ResultScanner rs = null;
          
		 rs = table.getScanner(scan);
	      table.close();
		for (Result r : rs) {// 按行去遍历
			String line="";
	        for (KeyValue kv : r.raw()) {// 遍历每一行的各列
	        	if(Bytes.toString(kv.getQualifier()).equals("record"))
	        	{
	        		line=Bytes.toString(kv.getValue());
	        		collector.emit(new Values(line));
	        	}
	          
	        	
	         
	        }
		}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		
		
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("source"));
	}

}
