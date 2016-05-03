package com.ai.baas.smc.calculate.topology.core.spout;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;

import com.ai.baas.smc.calculate.topology.core.bo.FinishListVo;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.helper.OptConfHelper;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

public class CalSpoutTest extends BaseRichSpout{
	
	private static Logger logger = LoggerFactory.getLogger(CalSpoutTest.class);

	 private SpoutOutputCollector collector;
	 //private static Connection connection;
	 private int i=0;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		//System.out.println("open--------------");
		   HBaseProxy.loadResource(conf);
		   HBaseProxy.getConnection();
		   Properties p=new Properties();
		   p.setProperty("ccs.appname", "baas-smc");
		   p.setProperty("ccs.zk_address", "10.1.130.84:39181");
		   OptConfHelper.loadPaaSConf(p);
		   this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if(i > 0){
			return;
		}
		logger.debug("spout开始..........");
		System.out.println("spout开始..........");
		ICacheClient cacheStatsTimes = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
		String finishlist = cacheStatsTimes.get(SmcCacheConstant.Cache.finishKey);
		System.out.println("-->"+finishlist);
		logger.debug("读取缓存结束..........");
		System.out.println("读取缓存结束..........");
		if(StringUtils.isBlank(finishlist)){
			return;
		}
		List<FinishListVo> voList = JSON.parseArray(finishlist,FinishListVo.class);
		System.out.println("-----------"+voList.size());
		
		for (FinishListVo vo : voList) {
			String tenantId = vo.getTenantId();
			String batchNo = vo.getBatchNo();
			//String billTimeSn = vo.getBillTimeSn();
			String billTimeSn = "201604";
			String objectId = vo.getObjectId();
			
//			StringBuilder bsnStr = new StringBuilder();
//			bsnStr.append("JS");
//			bsnStr.append(tenantId);
//			bsnStr.append(batchNo);	
			try {
				Table table = HBaseProxy.getConnection().getTable(TableName.valueOf("RTM_OUTPUT_DETAIL_" + billTimeSn));
				Scan scan = new Scan();
				scan.setFilter(new SingleColumnValueFilter("bsn".getBytes(),
						"value".getBytes(), CompareFilter.CompareOp.EQUAL,
						new BinaryComparator("JSBIUGZT20160301118".getBytes())));
				ResultScanner rs = table.getScanner(scan);
				for (Result r : rs) {// 按行去遍历
					String line = "";
					for (KeyValue kv : r.raw()) {// 遍历每一行的各列
						if (Bytes.toString(kv.getQualifier()).equals("record")) {
							line = Bytes.toString(kv.getValue());
							//System.out.println("---"+line);
							//collector.emit(new Values(line,objectId));
						}
					}
				}
				rs.close();
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			//test ........................................
			break;
			//test

		}
		i++;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("source","objectId"));
	}
	

}
