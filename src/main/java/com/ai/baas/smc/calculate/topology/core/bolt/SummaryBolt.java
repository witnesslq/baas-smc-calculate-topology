package com.ai.baas.smc.calculate.topology.core.bolt;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import com.ai.baas.smc.calculate.topology.core.bo.FinishListVo;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.util.RadisUtil;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;
import com.mysql.jdbc.Statement;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SummaryBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 8475030105476807164L;
	 private MappingRule[] mappingRules = new MappingRule[2];

	    private String[] outputFields = new String[] { "data" };
	
	private Connection connection;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context);
		try {
			connection=JdbcProxy.getConnection("");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String line="";
		try
		{
			line = input.getString(0);
			List<Object> values = null;
			String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);
			input.getString(0);	
			//业务处理 计数器加1
			ICacheClient cacheClient = CacheClientFactory
	                .getCacheClient(SmcCacheConstant.NameSpace.CAL_COMMON_CACHE);
			String counter=cacheClient.get(SmcCacheConstant.Cache.COUNTER);
			counter=String.valueOf(Integer.parseInt(counter)+1);
			String batchNo=input.getStringByField("batchNo");
			ICacheClient cacheStatsTimes = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
			String finishlist=cacheStatsTimes.get(SmcCacheConstant.Cache.finishKey);
			
			
            MessageParser messageParser = null;
            for (String inputData : inputDatas) {
                messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
                values = messageParser.toTupleData();
                if (CollectionUtils.isNotEmpty(values)) {
                    collector.emit(values);
                }
            }
            List<FinishListVo> voList=JSON.parseArray(finishlist, FinishListVo.class);
			for(FinishListVo vo:voList)
			{
				if(vo.getBatchNo().equals(batchNo))
				{
					if(vo.getStats_times().equals(counter))
					{
						
					}
				}
			
			}
		}
		catch(Exception e){
			
		}
	}

	
	
	public void insert() throws SQLException
	{
		 Statement st = null;
		 String tableName="";
		 st = (Statement) connection.createStatement();

		 String sql = "insert into "+tableName+"(bill_id,bill_from,batch_no,tenant_id,policy_code,stl_object_id,"
		 		+ "stl_element_id,stl_element_sn,bill_style_sn,bill_time_sn,bill_start_time,bill_end_time,orig_fee) values("+5+",'Tom'"+")";
         System.out.println("sql="+sql);
         int result = st.executeUpdate(sql);

	}
	
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
}
