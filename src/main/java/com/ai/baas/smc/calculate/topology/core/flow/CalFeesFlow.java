package com.ai.baas.smc.calculate.topology.core.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.calculate.topology.core.bolt.CostCalculatingBolt;
import com.ai.baas.smc.calculate.topology.core.spout.CalSpout;
import com.ai.baas.smc.calculate.topology.core.util.SmcConstants;
import com.ai.baas.storm.flow.BaseFlow;

public class CalFeesFlow extends BaseFlow {
    private static Logger logger = LoggerFactory.getLogger(CalFeesFlow.class);

    @Override
    public void define() {
        // 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
        builder.setSpout("SimpleSpout", new CalSpout(), 1);
        // 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
        builder.setBolt(SmcConstants.SIMPLE_BOLT, new CostCalculatingBolt(),
                getParallelNum("smc.calculate.executor.num", 1)).shuffleGrouping("SimpleSpout");

    }

    public static void main(String[] args) {
        logger.info("开始启动算费拓扑...");
        CalFeesFlow flow = new CalFeesFlow();
        flow.run(args);
    }

}
