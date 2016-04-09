package com.ai.baas.smc.calculate.topology.core.bolt;

import java.util.List;
import java.util.Map;

import com.ai.baas.smc.calculate.topology.core.proxy.CalculateProxy;
import com.ai.baas.storm.util.BaseConstants;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CostCalculatingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -3214008757998306486L;
	
	private  List paramList;
	
	private CalculateProxy calculateProxy;
	

	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		calculateProxy=new CalculateProxy();
		super.prepare(stormConf, context);
		
	}
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String line="";
		try
		{
			line=input.getStringByField(BaseConstants.RECORD_DATA);
			
			String objectId="";
			String tenantId="";
			calculateProxy.getPolicyList(objectId, tenantId);
			
		}
		catch(Exception e)
		{
			
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	

	

	

}
