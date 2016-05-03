package com.ai.baas.smc.calculate.topology.core.bolt;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItem;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.proxy.CalculateProxy;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.google.common.base.Joiner;

public class CostCalculatingBolt extends BaseBasicBolt {
	
	   private static final Logger LOG = LoggerFactory.getLogger(CostCalculatingBolt.class);

	private static final long serialVersionUID = -3214008757998306486L;

	private CalculateProxy calculateProxy;

	private MappingRule[] mappingRules = new MappingRule[2];

	private String[] outputFields;
	
	

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//HbaseClient.loadResource(stormConf);
		JdbcProxy.loadDefaultResource(stormConf);
		HBaseProxy.loadResource(stormConf);
		mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
		mappingRules[1] = mappingRules[0];
		
		calculateProxy = new CalculateProxy(stormConf);
		//super.prepare(stormConf, context);
		
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
		String bsn = "";
		try {
			String inputData = input.getString(0);
			//System.out.println("input===" + inputData);
			//LOG.info(" ====== 开始执行对账bolt，inputData = [" + inputData + "]");
			/* 1.获取并解析输入信息 */
			MessageParser messageParser = MessageParser.parseObject(inputData,mappingRules, outputFields);
			data = messageParser.getData();
			//System.out.println("data===" + data.toString());
			batchNo = StringUtils.defaultString(data.get("batch_no"));
			bsn = data.get(BaseConstants.BATCH_SERIAL_NUMBER);
			period = StringUtils.substring(data.get(BaseConstants.ACCOUNT_PERIOD), 0, 6);
			source = "sys";
			tenantId = data.get(BaseConstants.TENANT_ID);
			acctId = data.get(BaseConstants.ACCT_ID);

			objectId = StringUtils.upperCase(input.getStringByField("objectId"));
			List<StlPolicy> policyList = calculateProxy.getPolicyList(objectId,tenantId);
			//处理政策
			for (StlPolicy stlPolicy : policyList) {
				long elementId = stlPolicy.getStlElementId();
				String stlObjectId = stlPolicy.getStlObjectId();
				Long policyId = stlPolicy.getPolicyId();
				String billStyleSn = stlPolicy.getBillStyleSn();
				String elementSn = stlPolicy.getStlElementSn();
				List<StlPolicyItem> policyItemList = calculateProxy.getStlPolicyItemLists(policyId, tenantId);
				//处理政策结算项
				for (StlPolicyItem stlPolicyItem : policyItemList) {
					Long itemId = stlPolicyItem.getItemId();
					List<StlPolicyItemCondition> stlPolicyItemConditionList = calculateProxy.getPolicyItemList(itemId, tenantId);
					List<StlPolicyItemPlan> stlPolicyItemPlanList = calculateProxy.getStlPolicyItemPlan(itemId, tenantId);
					//匹配政策适配对象
					if (calculateProxy.matchPolicy(data,stlPolicyItemConditionList)) {
						for (StlPolicyItemPlan stlPolicyItemPlan : stlPolicyItemPlanList) {
							value = calculateProxy.caculateFees(stlPolicyItemPlan, data);
							//data.put("fee", String.valueOf(value));
							String billDataId = calculateProxy.dealBill(stlPolicy.getPolicyCode(),
									value, tenantId, batchNo, stlObjectId,
									elementId, billStyleSn, period,
									stlPolicyItemPlan.getFeeItem(),policyId.toString(),elementSn,bsn);
							String order_id = data.get("order_id");
							String[] family = new String[]{"data"};
							//long billDataId = calculateProxy.getBillDataId(stlPolicy.getPolicyCode());

							// 行键
							//租户ID_账单ID_账期ID_数据对象_账单来源_流水ID
							//需要增加政策ID
							String row = Joiner.on(BaseConstants.COMMON_JOINER)
									.join(tenantId, billDataId, period,
											objectId, source, order_id);
							
//							String row = tenantId + "_" + billDataId + "_"
//									+ period + "_" + objectId + "_" + source
//									+ "_" + order_id;
							
							//System.out.println("row===" + row);
							data.put("bill_id", billDataId);
							data.put("object_id",objectId);
							data.put("bill_from","sys");
							//String stlOrderDataKey = 
							
							data.put("stl_order_data_key", Joiner.on(BaseConstants.COMMON_JOINER).join(tenantId,batchNo,objectId,order_id));
							calculateProxy.outputDetailBill(period, row, data);
							
							System.out.println("bill_id="+billDataId);
							//HbaseClient.creatTable("stl_bill_detail_data_" + period, family);
							//HbaseClient.addRowByMap("stl_bill_detail_data_" + period, row, "data", messageParser.getData());
							//HbaseClient.addRowByMap("stl_bill_detail_data_" + period, row, "col_def", data);
						}
					}
				}
			}
			/**
			 * 更新计数器
			 */
			//calculateProxy.exportExcel(batchNo);
			ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.CAL_COMMON_CACHE);
			String counter = String.valueOf(cacheClient.hincrBy(SmcCacheConstant.Cache.COUNTER,bsn,1));
			String original = StringUtils.defaultString(cacheClient.hget(SmcCacheConstant.Cache.lockKey,bsn));
			if(original.equals(counter)){
				calculateProxy.insertBillData("stl_bill_data_" + period, "stl_bill_item_data_" + period,bsn);
				System.out.println("需要插入账单表喽。。。");
			}
			
//			ICacheClient cacheStatsTimes = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.STATS_TIMES);
//			String finishlist = cacheStatsTimes.get(SmcCacheConstant.Cache.finishKey);
//			List<FinishListVo> voList = JSON.parseArray(finishlist, FinishListVo.class);
//			for (FinishListVo vo : voList) {
//				if (vo.getBatchNo().equals(bsn)) {
//					if (vo.getStats_times().equals(counter)) {
//						calculateProxy.insertBillData("stl_bill_data_" + period, "stl_bill_item_data_" + period,bsn);
//						//calculateProxy.exportFileAndFtp(batchNo);
//						System.out.println("需要插入账单表喽。。。");
//					}
//				}
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
