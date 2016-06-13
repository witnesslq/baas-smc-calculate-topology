package com.ai.baas.smc.calculate.topology.core.spout;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ai.baas.smc.calculate.topology.core.bo.FinishListVo;
import com.ai.baas.smc.calculate.topology.core.util.LoadConfUtil;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.components.mcs.MCSClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

public class CalSpout extends BaseRichSpout {
    private static final long serialVersionUID = 5296876971073823644L;

    private static Logger LOG = LoggerFactory.getLogger(CalSpout.class);

    private SpoutOutputCollector collector;

    // private static Connection connection;

    @SuppressWarnings("unchecked")
    @Override
    public void open(@SuppressWarnings("rawtypes")
    Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LoadConfUtil.loadPaasConf(conf);
        HBaseProxy.loadResource(conf);
        HBaseProxy.getConnection();

        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        LOG.info("开始查询是否有算费数据...");
        ICacheClient cacheStatsTimes = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
        String finishlist = cacheStatsTimes.hget(SmcCacheConstant.NameSpace.STATS_TIMES,
                SmcCacheConstant.Cache.finishKey);
        if (StringUtils.isBlank(finishlist)) {
            return;
        }
        List<FinishListVo> voList = JSON.parseArray(finishlist, FinishListVo.class);
        System.out.println("[收到统计数]--->>" + voList.size());
        cacheStatsTimes.hdel(SmcCacheConstant.NameSpace.STATS_TIMES,
                SmcCacheConstant.Cache.finishKey);
        for (FinishListVo vo : voList) {
            int count = 0;
            String batchNo = vo.getBatchNo();
            String billTimeSn = vo.getBillTimeSn();
            String objectId = vo.getObjectId();
            System.out.println("[账期]--->>" + billTimeSn);
            System.out.println("[批次号]--->>" + batchNo);
            System.out.println("[objectId]--->>" + objectId);
            cacheStatsTimes.hset(SmcCacheConstant.Cache.lockKey, batchNo, vo.getStats_times());
            try {
                Table table = HBaseProxy.getConnection().getTable(
                        TableName.valueOf("RTM_OUTPUT_DETAIL_" + billTimeSn));
                Scan scan = new Scan();
                scan.setFilter(new SingleColumnValueFilter("bsn".getBytes(), "value".getBytes(),
                        CompareFilter.CompareOp.EQUAL, new BinaryComparator(batchNo.getBytes())));
                ResultScanner rs = table.getScanner(scan);
                for (Result r : rs) {// 按行去遍历
                    String line = "";
                    for (KeyValue kv : r.raw()) {// 遍历每一行的各列
                        if (Bytes.toString(kv.getQualifier()).equals("record")) {
                            line = Bytes.toString(kv.getValue());
                            // System.out.println("---"+line);
                            collector.emit(new Values(line, objectId));
                        }
                    }
                    count++;
                }
                rs.close();
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("从副本库[RTM_OUTPUT_DETAIL_" + billTimeSn + "]中取出" + count + "条数据!");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line", "objectId"));
    }

}
