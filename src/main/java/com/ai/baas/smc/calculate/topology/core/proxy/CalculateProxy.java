package com.ai.baas.smc.calculate.topology.core.proxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.api.policymanage.param.StepCalValue;
import com.ai.baas.smc.calculate.topology.core.bo.StlBillData;
import com.ai.baas.smc.calculate.topology.core.bo.StlBillItemData;
import com.ai.baas.smc.calculate.topology.core.bo.StlElement;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItem;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.bo.SwitchCalValue;
import com.ai.baas.smc.calculate.topology.core.util.CacheBLMapper;
import com.ai.baas.smc.calculate.topology.core.util.DateUtil;
import com.ai.baas.smc.calculate.topology.core.util.IKin;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.smc.calculate.topology.core.util.SmcConstants;
import com.ai.baas.smc.calculate.topology.core.util.SmcSeqUtil;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.sequence.datasource.SeqDataSourceLoader;
import com.ai.baas.storm.sequence.util.SeqUtil;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mysql.jdbc.Statement;

/**
 * 
 * @author zhangbc
 *
 */
public class CalculateProxy {
	
	private String detail_bill_prefix = "stl_bill_detail_data_";
	private String detail_bill_cf = "col_def";
	private String exportLocal = "~/export";
	private String exportRemotePath = "";
	

	public CalculateProxy(Map<String,String> stormConf){
		String localpath = stormConf.get("smc.calculate.export.local.temp");
		if(StringUtils.isNotBlank(localpath)){
			this.exportLocal = localpath;
		}
		initSeq(stormConf.get(BaseConstants.JDBC_DEFAULT));
	}
	
	private void initSeq(String jsonJdbcParam){
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject = (JsonObject)jsonParser.parse(jsonJdbcParam);
		Map<String,String> config = new HashMap<String,String>();
		for(Entry<String, JsonElement> entry:jsonObject.entrySet()){
			config.put(entry.getKey(), entry.getValue().getAsString());
		}
		SeqDataSourceLoader.initDefault(config);
	}

	/**
	 * 根据对象类型获取该对象下有效政策
	 * 
	 * @param objectId
	 * @param tenantId
	 * @return
	 * @throws ParseException
	 */
	public List<StlPolicy> getPolicyList(String objectId, String tenantId) throws Exception {
		// TODO Auto-generated method stub
		ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyAll = cacheClient.get(SmcCacheConstant.POLICY_ALL);
		List<StlPolicy> stlPolicyList = new ArrayList<StlPolicy>();
		List<StlPolicy> policyList = JSON.parseArray(policyAll, StlPolicy.class);
		for (int i = 0; i < policyList.size(); i++) {
			StlPolicy stlPolicy = (StlPolicy) policyList.get(i);
			if (stlPolicy.getStlObjectId().equals(objectId) && stlPolicy.getTenantId().equals(tenantId)) {
				stlPolicyList.add(stlPolicy);
			}
		}
		return stlPolicyList;
	}

	
	
	/**
	 * 
	 * @param policyId
	 * @param tenantId
	 * @return
	 */
	public List<StlPolicyItem> getStlPolicyItemLists(Long policyId, String tenantId)
	{
		ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll = cacheClient.get(SmcCacheConstant.POLICY_ITEM);
		List<StlPolicyItem> stlPolicyItemList = new ArrayList<StlPolicyItem>();
		List<StlPolicyItem> policyList = JSON.parseArray(policyItemAll, StlPolicyItem.class);
		for(StlPolicyItem stlPolicyItem:policyList){
			if(stlPolicyItem.getPolicyId().longValue()==policyId.longValue()&&stlPolicyItem.getTenantId().equals(tenantId))
			{
				stlPolicyItemList.add(stlPolicyItem);
			}
		}
		return stlPolicyItemList;
	}
	
	/**
	 * 获取政策适配对象
	 * 
	 * @param policyId
	 * @param tenantId
	 * @return
	 * @throws ParseException
	 */
	public List<StlPolicyItemCondition> getPolicyItemList(Long itemId, String tenantId) throws Exception {
		// TODO Auto-generated method stub
		ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll = cacheClient.get(SmcCacheConstant.POLICY_ITEM_CONDITION);
		List<StlPolicyItemCondition> stlPolicyItemConditionList = new ArrayList<StlPolicyItemCondition>();
		List<StlPolicyItemCondition> policyList = JSON.parseArray(policyItemAll, StlPolicyItemCondition.class);
		for (StlPolicyItemCondition stlPolicyItemCondition : policyList) {
			if (stlPolicyItemCondition.getItemId().longValue() == itemId.longValue()
					&& stlPolicyItemCondition.getTenantId().equals(tenantId)) {
				stlPolicyItemConditionList.add(stlPolicyItemCondition);
			}
		}
		return stlPolicyItemConditionList;
	}

	public List<StlPolicyItemPlan> getStlPolicyItemPlan(Long itemId, String tenantId) throws Exception {
		ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll = cacheClient.get(SmcCacheConstant.POLICY_ITEM_PLAN);
		List<StlPolicyItemPlan> stlPolicyItemPlanList = new ArrayList<StlPolicyItemPlan>();
		List<StlPolicyItemPlan> policyList = JSON.parseArray(policyItemAll, StlPolicyItemPlan.class);
		for (int i = 0; i < policyList.size(); i++) {
			StlPolicyItemPlan stlPolicyItemPlan = (StlPolicyItemPlan) policyList.get(i);
			if (stlPolicyItemPlan.getItemId().longValue() == itemId.longValue() && stlPolicyItemPlan.getTenantId().equals(tenantId)) {
				stlPolicyItemPlanList.add(stlPolicyItemPlan);
			}
		}
		return stlPolicyItemPlanList;
	}

//	public List getParamList() {
//		IDshmClient client = null;
//		if (client == null)
//			client = new DshmClient();
//		ICacheClient cacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
//		Map<String, String> params = new TreeMap<String, String>();
//		params.put("price_code", "999");
//		params.put("tenant_id", "VIV-BYD");
//		List<Map<String, String>> results = client.list("cp_price_info").where(params).executeQuery(cacheClient);
//		return results;
//	}

	/**
	 * 是否匹配规则
	 * 
	 * @param stream
	 * @param paramList
	 * @param policyItemList
	 * @return
	 */
	public boolean matchPolicy(Map<String,String> data, List<StlPolicyItemCondition> policyItemList) {
		boolean flag = false;
		ICacheClient elementcacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);
		for (StlPolicyItemCondition stlPolicyItemCondition : policyItemList) {
			StringBuilder elementStr = new StringBuilder();
			elementStr.append(stlPolicyItemCondition.getTenantId());
			elementStr.append(".");
			elementStr.append(String.valueOf(stlPolicyItemCondition.getElementId()));
			//String compare =(String)data.get("content");
			String elementJson = elementcacheClient.get(elementStr.toString());
			if(StringUtils.isBlank(elementJson)){
				break;
			}
			StlElement stlElement = JSON.parseObject(elementJson, StlElement.class);
			String compare = "";
			if(stlElement.getAttrType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_ATTR_TYPE_STAT)){
				if(stlElement.getStatisticsType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_STAT_TYPE_C_CNT)){
					int len = StringUtils.defaultString(data.get("content")).length();
					compare = String.valueOf(len);
				}else{
					System.out.println("内存中目前没有统计数据，敬请期待。。。");
					flag = true;
					continue;
				}
			}else{
				String elementCode = stlElement.getElementCode();
				compare = data.get(elementCode);
			}

			String matchType = stlPolicyItemCondition.getMatchType();
			String matchValue = stlPolicyItemCondition.getMatchValue();
			
			if (matchType.equals("in")) {
				flag = IKin.in(matchValue, compare);
			} else if (matchType.equals("nin")) {
				flag = !IKin.in(matchValue, compare);
			} else {
				if(matchType.equals("=")){
					matchType = "==";
				}
				String expression = "a" + matchType + "b";
				List<Variable> variables = new ArrayList<Variable>();
				variables.add(Variable.createVariable("a", compare));
				variables.add(Variable.createVariable("b", matchValue));
				Object result = ExpressionEvaluator.evaluate(expression, variables);
				flag = Boolean.parseBoolean(result.toString());
			}
			
			//只要存在条件不满足,就退出
			if(!flag){
				break;
			}
		}

		//System.out.println("flag====="+flag);
		return flag;
	}

//	public Map valueMap(String[] stream, List paramList) {
//		Map map = new HashMap();
//		for (int i = 0; i < stream.length; i++) {
//			for (int j = 0; j < paramList.size(); j++) {
//				Map paramMap = (Map) paramList.get(j);
//				if (paramMap.containsKey(i)) {
//					map.put(paramMap.get(i), stream[i]);
//				}
//			}
//		}
//		return map;
//
//	}

	public double caculateFees(StlPolicyItemPlan stlPolicyItemPlan, Map<String,String> data) {
		double value = docaculate(stlPolicyItemPlan, data);
		data.put("item_fee", String.valueOf(value));
		data.put("fee_item_id", stlPolicyItemPlan.getFeeItem());
		return value;
	}

	/**
	 * 算费
	 * 
	 * @param planType
	 * @param calType
	 * @return
	 */
	public double docaculate(StlPolicyItemPlan policyDetailQueryPlanInfo,Map<String,String> data) {
		double value = 0;
		ICacheClient elementcacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);
		String planType = policyDetailQueryPlanInfo.getPlanType();
		String calType = policyDetailQueryPlanInfo.getCalType();// 算费方式
		Long elementId = policyDetailQueryPlanInfo.getElementId();

		StringBuilder elementStr = new StringBuilder();
		elementStr.append(policyDetailQueryPlanInfo.getTenantId());
		elementStr.append(".");
		elementStr.append(elementId);
		
		String elementJson = elementcacheClient.get(elementStr.toString());
		StlElement stlElement=JSON.parseObject(elementJson, StlElement.class);
//		long sortId = stlElement.getSortId();
//		int num = (int) sortId;
		String compare = "";
		if(stlElement.getAttrType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_ATTR_TYPE_STAT)){
			if(stlElement.getStatisticsType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_STAT_TYPE_C_CNT)){
				int len = StringUtils.defaultString(data.get("content")).length();
				compare = String.valueOf(len);
			}else{
				System.out.println("内存中目前没有统计数据，敬请期待。。。");
				return value;
			}
		}else{
			String elementCode = stlElement.getElementCode();
			compare = data.get(elementCode);
		}
		//compare =(String)data.get("content");
		if (planType.equals("normal")) {// 标准型
			//可能出现错误，calValue是否是JSON，如果是需要解析转换后使用
			double calValue = Double.parseDouble(policyDetailQueryPlanInfo.getCalValue());
			if (calType.equals("ratio"))// 按比例
			{
				value = Double.parseDouble(compare) * calValue;
			} else if (calType.equals("fixed"))// 固定值
			{
				value = calValue;
			} else if (calType.equals("price"))// 单价
			{
				value = Double.parseDouble(compare) * calValue;
			}

		} else if (planType.equals("step")) {// 阶梯
			List<StepCalValue> stepCalValues = JSON.parseArray(policyDetailQueryPlanInfo.getCalValue(), StepCalValue.class);
			String calValue = "";
			for (StepCalValue stepCalValue : stepCalValues) {
				double start = Double.parseDouble(stepCalValue.getStartValue());
				double end = Double.parseDouble(stepCalValue.getEndValue());
				if (end < value && start < value) {
					calValue = stepCalValue.getCalValue();
					value += Double.parseDouble(calValue) * (end - start);
				}
				if (value > start && value < end) {
					calValue = stepCalValue.getCalValue();
					value += Double.parseDouble(calValue) * (Double.parseDouble(compare) - start);
				}
				if (calType.equals("ratio")) {
					value = Double.parseDouble(compare) * value;
				} else if (calType.equals("price")) {
					value = Double.parseDouble(compare) * Double.parseDouble(calValue)*value;
				}

			}
		} else if ("switch".equals(planType)) {// 分档

			List<SwitchCalValue> stepCalValues = JSON.parseArray(policyDetailQueryPlanInfo.getCalValue(),SwitchCalValue.class);
			String calValue = "0";
			double compareValue = Double.parseDouble(compare);
			for (SwitchCalValue stepCalValue : stepCalValues) {
				double start = Double.parseDouble(stepCalValue.getStart_value());
				double end = Double.parseDouble(stepCalValue.getEnd_value());
				if (compareValue > start && compareValue < end) {
					calValue = stepCalValue.getValue();
				}
			}
			if (calType.equals("ratio")) {
				value = compareValue * Double.parseDouble(calValue);
			} else if (calType.equals("fixed")) {
				value = Double.parseDouble(calValue);
			} else if (calType.equals("price")) {
				value = compareValue * Double.parseDouble(calValue);
			}
		}
		
		return value;
	}

	

	public synchronized String dealBill(String policyCode, double value, String tenantId, String batchNo, String objectId,
			long elementId, String billStyle, String billTime,String feeItemId,String policyId,String elementSn,String bsn) throws Exception{
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		//一级key=SMC_BILL_账期     二级key=租户+批次号+账期+政策+科目..................
		//String billAll = billClient.get(getCacheBillKey(policyId));
		String billAll = billClient.hget(getCacheBillTable(bsn), policyId);
		StlBillData stlBillData = null;
		if (StringUtils.isBlank(billAll)) {
			stlBillData = new StlBillData();
			stlBillData.setPolicyCode(policyCode);
			stlBillData.setTenantId(tenantId);
			stlBillData.setBatchNo(batchNo);
			stlBillData.setStlObjectId(objectId);
			stlBillData.setStlElementId(elementId);
			stlBillData.setStlElementSn(elementSn);
			stlBillData.setBillStyleSn(billStyle);
			stlBillData.setOrigFee(new Double(0));
			//stlBillData.setFeeItemId(feeItemId);
			//stlBillData.setBillId(Long.parseLong(SmcSeqUtil.getRandom()));
			stlBillData.setBillId(SeqUtil.getNewId(SmcConstants.STL_BILL_DATA$BILL_ID$SEQ));
			stlBillData.setBillFrom("sys");
			stlBillData.setBillStartTime(DateUtil.getFirstDay(billTime, "yyyyMM"));
			stlBillData.setBillEndTime(DateUtil.getLastDay(billTime, "yyyyMM"));
			stlBillData.setBillTimeSn(billTime);
			stlBillData.setItemDatas(new ArrayList<StlBillItemData>());
		}else{
			stlBillData = JSON.parseObject(billAll, StlBillData.class);
//			double fee = stlBillData.getOrigFee().doubleValue() + value;
//			stlBillData.setOrigFee(fee);
		}
		billClient.hincrByFloat(SmcCacheConstant.Cache.BILL_DATA_PREFIX+bsn, policyId, value);
		List<StlBillItemData> itemDatas = stlBillData.getItemDatas();
		if (!contains(itemDatas, feeItemId, value)) {
			StlBillItemData stlBillItemData = new StlBillItemData();
			stlBillItemData.setBillId(stlBillData.getBillId());
			stlBillItemData.setTenantId(stlBillData.getTenantId());
			stlBillItemData.setItemType("1");
			stlBillItemData.setFeeItemId(feeItemId);
			stlBillItemData.setTotalFee(new Double(0));
			itemDatas.add(stlBillItemData);
		}
		billClient.hincrByFloat(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX+bsn, policyId+":"+feeItemId, value);
		//billClient.set(getCacheBillTable(policyId), JSON.toJSONString(stlBillData));
		billClient.hset(getCacheBillTable(bsn), policyId, JSON.toJSONString(stlBillData));
		return stlBillData.getBillId().toString();
	}

	boolean contains(List<StlBillItemData> itemDatas, String feeItemId, double value){
		for (StlBillItemData stlBillItemData : itemDatas) {
			if(feeItemId.equals(stlBillItemData.getFeeItemId())){
//				double addup = stlBillItemData.getTotalFee().doubleValue() + value;
//				stlBillItemData.setTotalFee(new Double(addup));
				return true;
			}
		}
		return false;
	}
	
	
//	boolean contains(String policyCode, List<StlBillData> dataList) {
//		boolean flag = false;
//		for (StlBillData stlBillData : dataList) {
//			if (stlBillData.getPolicyCode().equals(policyCode)) {
//				flag = true;
//			}
//		}
//		return flag;
//	}
	
	private String getCacheBillTable(String batchNo){
		StringBuilder billKey = new StringBuilder();
		billKey.append(SmcCacheConstant.Cache.BILL_PREFIX).append(batchNo);
		return billKey.toString();	
	}
	
	public void outputDetailBill(String period,String row, Map<String,String> data){
		try {
			TableName tableName = TableName.valueOf(detail_bill_prefix+period);
			Table table = HBaseProxy.getConnection().getTable(tableName);
			Put put = new Put(Bytes.toBytes(row));
			byte[] cf = Bytes.toBytes(detail_bill_cf);
			for (Entry<String, String> entry : data.entrySet()) {
				put.addColumn(cf, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
			}
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public long getBillDataId(String policyCode) {
		long billDataId=0;
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		String billAll = billClient.get("bill");
		List<StlBillData> dataList = JSON.parseArray(billAll, StlBillData.class);
		for (StlBillData stlBillData : dataList) {
			if(stlBillData.getPolicyCode().equals(policyCode))
			{
				billDataId=	stlBillData.getBillId();
			}
		}
		return billDataId;
	}
	
	
	public void insertBillData(String period,String bsn) throws Exception
	{
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		Map<String,String> billMaps = billClient.hgetAll(SmcCacheConstant.Cache.BILL_PREFIX+bsn);
		int opt_times = 0;
		StlBillData stlBillData = null;
		//for(String bill:billMaps.values()){
		for (Entry<String, String> entry : billMaps.entrySet()) {
			stlBillData = JSON.parseObject(entry.getValue(), StlBillData.class);
			if(insert(stlBillData,period,entry.getKey(),bsn,billClient)){
				opt_times++;
			}
		}
		if(billMaps.size() == opt_times){
			//导出文件
			billClient.del(SmcCacheConstant.Cache.BILL_PREFIX+bsn);
			billClient.del(SmcCacheConstant.Cache.BILL_DATA_PREFIX+bsn);
			billClient.del(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX+bsn);
			billClient.hdel(SmcCacheConstant.Cache.lockKey, bsn);
			billClient.hdel(SmcCacheConstant.Cache.COUNTER, bsn);
		}
	}
	
	public boolean insert(StlBillData stlBillData,String period,String policyId,String bsn,ICacheClient client){
		Connection conn = null;
		boolean isSucc = false;
		try{
			conn = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT);
			conn.setAutoCommit(false);
			String createTime = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
			Statement statement = (Statement)conn.createStatement();
			String origFee = client.hget(SmcCacheConstant.Cache.BILL_DATA_PREFIX+bsn, policyId);
			//System.out.println("origFee=="+origFee);
			insertBillData(stlBillData, period, createTime, statement, origFee);
	        List<StlBillItemData> itemDatas = stlBillData.getItemDatas();
	        String billId = stlBillData.getBillId().toString();
	        String totalFee = "0";
	        for(StlBillItemData itemData:itemDatas){
	        	totalFee = client.hget(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX+bsn, policyId+":"+itemData.getFeeItemId());
	        	//System.out.println("totalFee---"+totalFee);
				insertBillItemData(itemData, billId, period, createTime,
						statement, totalFee);
	        }
			conn.commit();
			isSucc = true;
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
		return isSucc;
	}
	
	private int insertBillItemData(StlBillItemData itemData, String billId,
			String period, String createTime, Statement statement,
			String totalFee) throws SQLException {
		String billItemId = ObjectUtils.toString(SeqUtil.getNewId(SmcConstants.STL_BILL_ITEM_DATA$BILL_ITEM_ID$SEQ),SmcSeqUtil.getRandom());
		StringBuilder strSql = new StringBuilder("insert into ");
		strSql.append(SmcConstants.STL_BILL_ITEM_DATA_TABLE_PREFIX).append(period);
		strSql.append(" (bill_item_id,bill_id,tenant_id,item_type,fee_item_id,total_fee,create_time)");
		strSql.append(" values(");
		strSql.append(billItemId).append(",");
		strSql.append(billId).append(",'");
		strSql.append(itemData.getTenantId()).append("','");
		strSql.append(itemData.getItemType()).append("','");
		strSql.append(itemData.getFeeItemId()).append("',");
		strSql.append(totalFee).append(",");
		strSql.append(createTime).append(")");
		System.out.println("itemSql="+strSql);
		return statement.executeUpdate(strSql.toString());
	}
	
	private int insertBillData(StlBillData stlBillData, String period,
			String createTime, Statement statement, String origFee) throws SQLException {
		
		StringBuilder strSql = new StringBuilder("insert into ");
		strSql.append(SmcConstants.STL_BILL_DATA_TABLE_PREFIX).append(period);
		strSql.append(" (bill_id,bill_from,batch_no,tenant_id,policy_code,stl_object_id,");
		strSql.append("stl_element_id,stl_element_sn,bill_style_sn,bill_time_sn,");
		strSql.append("bill_start_time,bill_end_time,orig_fee,create_time)");
		strSql.append(" values(");
		strSql.append(stlBillData.getBillId()).append(",'");
		strSql.append(stlBillData.getBillFrom()).append("','");
		strSql.append(stlBillData.getBatchNo()).append("','");
		strSql.append(stlBillData.getTenantId()).append("','");
		strSql.append(stlBillData.getPolicyCode()).append("','");
		strSql.append(stlBillData.getStlObjectId()).append("',");
		strSql.append(stlBillData.getStlElementId()).append(",'");
		strSql.append(stlBillData.getStlElementSn()).append("','");
		strSql.append(stlBillData.getBillStyleSn()).append("',");
		strSql.append(stlBillData.getBillTimeSn()).append(",'");
		strSql.append(stlBillData.getBillStartTime()).append("','");
		strSql.append(stlBillData.getBillEndTime()).append("',");
		strSql.append(origFee).append(",");
		strSql.append(createTime).append(")");
		
		System.out.println("sql="+strSql);
        return statement.executeUpdate(strSql.toString());
	}
	
	
	
	public void exportFileAndFtp(String batchNo){
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		Map<String,String> billAll= billClient.hgetAll("smc_bill_"+batchNo);
		StlBillData stlBillData = null;
		//for(String bill:billAll.values()){
		String policyId = "",billJson = "";
		String exportPath = "";
		for(Entry<String,String> entry:billAll.entrySet()){
			policyId = entry.getKey();
			stlBillData = JSON.parseObject(entry.getValue(), StlBillData.class);
			try {
				exportPath = exportExcel(stlBillData,policyId,batchNo);
				exportCsv(stlBillData,policyId);
				//不用政策Id导出，用账单ID作为导出id
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	public String exportCsv(StlBillData stlBillData,String policyId){
		String period = stlBillData.getBillTimeSn();
		TableName tableName = TableName.valueOf(detail_bill_prefix+period);
		try {
			Table table = HBaseProxy.getConnection().getTable(tableName);
			FilterList filterList = new FilterList();
			filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(stlBillData.getTenantId())));
			filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(policyId)));
			filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(period)));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			for (Result res : scanner) {
				//res.
				
				
//				Cell[] cells = res.rawCells();
//				for(Cell cell:cells){
//					System.out.println(Bytes.toString(cell.getQualifier())+"="+Bytes.toString(cell.getValue()));
//				}
				
			}
			scanner.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return "";
	}
	
	
	
	public String exportExcel(StlBillData stlBillData,String policyId,String batchNo) throws Exception{
		Workbook wb = new XSSFWorkbook();
        XSSFCellStyle cellStyle = (XSSFCellStyle) wb.createCellStyle();
        cellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
        cellStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        cellStyle.setAlignment(HSSFCellStyle.ALIGN_LEFT);
        
        XSSFSheet sheet0 = (XSSFSheet) wb.createSheet("账单");
        XSSFRow row0 = sheet0.createRow(0);// 第一行
        XSSFCell cell = row0.createCell(0);
        cell.setCellValue("结算方");
        cell.setCellStyle(cellStyle);
        cell = row0.createCell(1);
        cell.setCellValue(stlBillData.getBillFrom());
        cell = row0.createCell(2);
        cell.setCellValue("批次号");
        cell.setCellStyle(cellStyle);
        cell = row0.createCell(3);
        cell.setCellValue(stlBillData.getBatchNo());
        
        XSSFRow row1 = sheet0.createRow(1);// 第二行
        cell = row1.createCell(0);
        cell.setCellValue("政策编码");
        cell.setCellStyle(cellStyle);
        cell = row1.createCell(1);
        cell.setCellValue(stlBillData.getPolicyCode());
        cell = row1.createCell(2);
        cell.setCellValue("账期");
        cell.setCellStyle(cellStyle);
        cell = row1.createCell(3);
        cell.setCellValue(stlBillData.getBillTimeSn());
 
        XSSFRow row2 = sheet0.createRow(2);// 第三行
        cell = row2.createCell(0);
        cell.setCellValue("开始时间");
        cell.setCellStyle(cellStyle);
        cell = row2.createCell(1);
        cell.setCellValue(stlBillData.getBillStartTime().toString());
        cell = row2.createCell(2);
        cell.setCellValue("结束时间");
        cell.setCellStyle(cellStyle);
        cell = row2.createCell(3);
        cell.setCellValue(stlBillData.getBillEndTime().toString());
        
        XSSFRow row3 = sheet0.createRow(3);// 第四行
        cell = row3.createCell(0);
        cell.setCellValue("结算金额(元)");
        cell.setCellStyle(cellStyle);
        cell = row3.createCell(1);
        cell.setCellValue(stlBillData.getOrigFee().toString());
        
        XSSFRow row5 = sheet0.createRow(5);// 第六行
        cell = row5.createCell(0);
        cell.setCellValue("科目ID");
        cell.setCellStyle(cellStyle);
        cell = row5.createCell(1);
        cell.setCellValue("科目名称");
        cell.setCellStyle(cellStyle);
        cell = row5.createCell(2);
        cell.setCellValue("总金额(元)");
        cell.setCellStyle(cellStyle);
        
        int lineNo = 6;
        List<StlBillItemData> itemDatas = stlBillData.getItemDatas();
		for (StlBillItemData itemData : itemDatas) {
        	XSSFRow rowTmp = sheet0.createRow(lineNo);
            cell = rowTmp.createCell(0);
            cell.setCellValue(itemData.getFeeItemId());
            cell = rowTmp.createCell(1);
            cell.setCellValue("科目名称");
            cell = rowTmp.createCell(2);
            cell.setCellValue(itemData.getTotalFee().toString());
        	lineNo++;
        }

		String fileName = Joiner
				.on(BaseConstants.COMMON_JOINER)
				.join(stlBillData.getTenantId(), stlBillData.getPolicyCode(),
						stlBillData.getBillTimeSn()).concat(".xlsx");
        
		String local = Joiner.on(File.separator).join(exportLocal, batchNo, policyId);
        
        FileUtils.forceMkdir(FileUtils.getFile(local.toString()));
        
		FileOutputStream fileOut = new FileOutputStream(local+File.separator+fileName);
		wb.write(fileOut);
		
		return local.toString();
	}
	

}
