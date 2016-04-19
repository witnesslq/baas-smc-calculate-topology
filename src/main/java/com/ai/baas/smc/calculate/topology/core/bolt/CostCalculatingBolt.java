package com.ai.baas.smc.calculate.topology.core.bolt;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.calculate.topology.core.bo.FinishListVo;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItem;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.proxy.CalculateProxy;
import com.ai.baas.smc.calculate.topology.core.util.HbaseClient;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CostCalculatingBolt extends BaseBasicBolt {
	
	   private static final Logger LOG = LoggerFactory.getLogger(CostCalculatingBolt.class);

	private static final long serialVersionUID = -3214008757998306486L;

	private CalculateProxy calculateProxy;

	private MappingRule[] mappingRules = new MappingRule[2];

	private String[] outputFields;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
		mappingRules[1] = mappingRules[0];
		calculateProxy = new CalculateProxy();
		HbaseClient.loadResource(stormConf);
		JdbcProxy.loadDefaultResource(stormConf);

		super.prepare(stormConf, context);

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		 Map<String, String> data = null;
		 
		String line = "";
		double value = 0;
		String period = "";
		String tenantId = "";
		String acctId = "";
		String objectId = "";
		String batchNo = "";
		String source = "";
		try {
			  String inputData = input.getString(0);
	            LOG.info(" ====== 开始执行对账bolt，inputData = [" + inputData + "]");
	            /* 1.获取并解析输入信息 */
	            MessageParser messageParser = MessageParser.parseObject(inputData, mappingRules,
	                    outputFields);
	            data = messageParser.getData();
			batchNo = data.get(BaseConstants.BATCH_SERIAL_NUMBER);
			period = data.get(BaseConstants.ACCOUNT_PERIOD);
			source = data.get(BaseConstants.SOURCE);
			tenantId = data.get(BaseConstants.TENANT_ID);
			acctId =data.get(BaseConstants.ACCT_ID);

			objectId = input.getStringByField("objectId");
			List<StlPolicy> policyList = calculateProxy.getPolicyList(objectId, tenantId);
			for (StlPolicy stlPolicy : policyList) {
				long elementId = stlPolicy.getStlElementId();
				String stlObjectId = stlPolicy.getStlObjectId();
				Long policyId = stlPolicy.getPolicyId();
				String billStyleSn = stlPolicy.getBillStyleSn();
				List<StlPolicyItem> policyItemList= calculateProxy.getStlPolicyItemLists(policyId,tenantId);
				for(StlPolicyItem stlPolicyItem:policyItemList)
				{
				Long itemId=stlPolicyItem.getItemId();
				List<StlPolicyItemCondition> stlPolicyItemConditionList = calculateProxy.getPolicyItemList(itemId,
						tenantId);
				List<StlPolicyItemPlan> stlPolicyItemPlanList = calculateProxy.getStlPolicyItemPlan(itemId, tenantId);
				if (calculateProxy.matchPolicy(data, stlPolicyItemConditionList)) {
					for (StlPolicyItemPlan stlPolicyItemPlan : stlPolicyItemPlanList) {
						value = calculateProxy.caculateFees(stlPolicyItemPlan, data);
						calculateProxy.dealBill(stlPolicy.getPolicyCode(), value, tenantId, batchNo, stlObjectId,
								elementId, billStyleSn, period,stlPolicyItemPlan.getFeeItem());
						line = line + BaseConstants.FIELD_SPLIT + value;
						List<Object> values = null;
						MessageParser.parseObject(line, mappingRules, outputFields);
						String order_id = data.get("order_id");
						values = messageParser.toTupleData();
						if (CollectionUtils.isNotEmpty(values)) {
							collector.emit(values);
						}
						String[] family = new String[0];
						family[0] = "data";
						long billDataId = calculateProxy.getBillDataId(stlPolicy.getPolicyCode());
						
						//行键
						String row = tenantId + "_" + billDataId + "_" + period + "_" + objectId + "_" + source + "_"
								+ order_id;

						HbaseClient.creatTable("stl_bill_detail_data_" + period, family);
						HbaseClient.addRowByMap("stl_bill_detail_data_" + period, row, "data", messageParser.getData());
					}

				}
			}
			}
			/**
			 * 更新计数器
			 */
			ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.CAL_COMMON_CACHE);
			String counter = cacheClient.get(SmcCacheConstant.Cache.COUNTER);
			counter = String.valueOf(Integer.parseInt(counter) + 1);
			ICacheClient cacheStatsTimes = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
			String finishlist = cacheStatsTimes.get(SmcCacheConstant.Cache.finishKey);
			List<FinishListVo> voList = JSON.parseArray(finishlist, FinishListVo.class);
			for (FinishListVo vo : voList) {
				if (vo.getBatchNo().equals(batchNo)) {
					if (vo.getStats_times().equals(counter)) {
						calculateProxy.insertBillData("stl_bill_data_"+period,"stl_bill_item_data_"+period);
					}
				}

			}
		} catch (Exception e) {

		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
