package com.ai.baas.smc.calculate.topology.core.flow;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.calculate.topology.core.bolt.CostCalculatingBolt;
import com.ai.baas.smc.calculate.topology.core.bolt.UnpackingBolt;
import com.ai.baas.smc.calculate.topology.core.spout.CalSpout;
import com.ai.baas.smc.calculate.topology.core.util.BmcConstants;
import com.ai.baas.storm.flow.BaseFlow;
import com.ai.baas.storm.util.BaseConstants;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;


/**
 * GPRS通用拓扑图
 * @author majun
 * @since 2016.3.16
 */
public class CalFeesFlow extends BaseFlow {
	private static Logger logger = LoggerFactory.getLogger(CalFeesFlow.class);
	
	@Override
	@SuppressWarnings("unchecked")
	public void define() {
        // 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
		builder.setSpout("SimpleSpout", new CalSpout(), 1);
        // 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
		builder.setBolt("SimpleBolt", new CostCalculatingBolt(), 1).shuffleGrouping("SimpleSpout");
		
      
		
		
	}

	public static void main(String[] args) {
		CalFeesFlow flow = new CalFeesFlow();
		flow.run(args);
	}

	
}
