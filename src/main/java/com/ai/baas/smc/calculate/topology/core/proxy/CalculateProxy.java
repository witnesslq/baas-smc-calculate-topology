package com.ai.baas.smc.calculate.topology.core.proxy;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.api.policymanage.param.StepCalValue;
import com.ai.baas.smc.calculate.topology.core.bo.StlBillData;
import com.ai.baas.smc.calculate.topology.core.bo.StlElement;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItem;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.util.CacheBLMapper;
import com.ai.baas.smc.calculate.topology.core.util.IKin;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.smc.calculate.topology.core.util.SmcSeqUtil;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.dubbo.common.json.ParseException;
import com.alibaba.fastjson.JSON;
import com.mysql.jdbc.Statement;

/**
 * 
 * @author zhangbc
 *
 */
public class CalculateProxy {

	public String cal(String[] stream) {

		return null;
	}

	/**
	 * 根据对象类型获取该对象下有效政策
	 * 
	 * @param objectId
	 * @param tenantId
	 * @return
	 * @throws ParseException
	 */
	public List<StlPolicy> getPolicyList(String objectId, String tenantId) throws ParseException {
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
			if(stlPolicyItem.getPolicyId()==policyId&&stlPolicyItem.getTenantId().equals(tenantId))
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
	public List<StlPolicyItemCondition> getPolicyItemList(Long itemId, String tenantId) throws ParseException {
		// TODO Auto-generated method stub
		ICacheClient cacheClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll = cacheClient.get(SmcCacheConstant.POLICY_ITEM_CONDITION);
		List<StlPolicyItemCondition> stlPolicyItemConditionList = new ArrayList<StlPolicyItemCondition>();
		List<StlPolicyItemCondition> policyList = JSON.parseArray(policyItemAll, StlPolicyItemCondition.class);
		for (StlPolicyItemCondition stlPolicyItemCondition : policyList) {
			if (stlPolicyItemCondition.getItemId() == itemId
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
			if (stlPolicyItemPlan.getItemId() == itemId && stlPolicyItemPlan.getTenantId().equals(tenantId)) {
				stlPolicyItemPlanList.add(stlPolicyItemPlan);
			}
		}
		return stlPolicyItemPlanList;
	}

	public List getParamList() {
		IDshmClient client = null;
		if (client == null)
			client = new DshmClient();
		ICacheClient cacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
		Map<String, String> params = new TreeMap<String, String>();
		params.put("price_code", "999");
		params.put("tenant_id", "VIV-BYD");
		List<Map<String, String>> results = client.list("cp_price_info").where(params).executeQuery(cacheClient);
		return results;
	}

	/**
	 * 是否匹配规则
	 * 
	 * @param stream
	 * @param paramList
	 * @param policyItemList
	 * @return
	 */
	public boolean matchPolicy(Map data, List<StlPolicyItemCondition> policyItemList) {

		boolean flag = true;
		 ICacheClient elementcacheClient = CacheClientFactory
	                .getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);

		for (StlPolicyItemCondition stlPolicyItemCondition : policyItemList) {
			String matchType = stlPolicyItemCondition.getMatchType();
			String matchValue = stlPolicyItemCondition.getMatchValue();
	
			String compare =(String)data.get("content");

			if (matchType.equals("in")) {
				flag = IKin.in(matchValue, compare);

			} else if (matchType.equals("nin")) {
				flag = !IKin.in(matchValue, compare);
			} else {
				String expression = "a" + matchType + "b";
				List<Variable> variables = new ArrayList<Variable>();
				variables.add(Variable.createVariable("a", compare));
				variables.add(Variable.createVariable("b", matchValue));
				Object result = ExpressionEvaluator.evaluate(expression, variables);
				flag = Boolean.parseBoolean(result.toString());
			}
		}

		return flag;
	}

	public Map valueMap(String[] stream, List paramList) {
		Map map = new HashMap();
		for (int i = 0; i < stream.length; i++) {
			for (int j = 0; j < paramList.size(); j++) {
				Map paramMap = (Map) paramList.get(j);
				if (paramMap.containsKey(i)) {
					map.put(paramMap.get(i), stream[i]);
				}
			}
		}
		return map;

	}

	public double caculateFees(StlPolicyItemPlan stlPolicyItemPlan, Map data) {
		double value = 0;
		value = docaculate(stlPolicyItemPlan, data);
		return value;
	}

	/**
	 * 算费
	 * 
	 * @param planType
	 * @param calType
	 * @return
	 */
	public double docaculate(StlPolicyItemPlan policyDetailQueryPlanInfo,Map data) {
		double value = 0;
		 ICacheClient elementcacheClient = CacheClientFactory
	                .getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);
		String planType = policyDetailQueryPlanInfo.getPlanType();
		String calType = policyDetailQueryPlanInfo.getCalType();// 算费方式
		Long elementId = policyDetailQueryPlanInfo.getElementId();

		String elementALl=elementcacheClient.get(policyDetailQueryPlanInfo.getTenantId()+"."+elementId);
		StlElement stlElement=JSON.parseObject(elementALl, StlElement.class);
		long sortId = stlElement.getSortId();
		int num = (int) sortId;
		String compare =(String)data.get("content");
		
		
		
		if (planType.equals("nomal")) {// 标准型
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
			List<StepCalValue> stepCalValues = JSON.parseArray(policyDetailQueryPlanInfo.getCalValue(),
					StepCalValue.class);
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

			List<StepCalValue> stepCalValues = JSON.parseArray(policyDetailQueryPlanInfo.getCalValue(),
					StepCalValue.class);
			String calValue = "";
			for (StepCalValue stepCalValue : stepCalValues) {
				double start = Double.parseDouble(stepCalValue.getStartValue());
				double end = Double.parseDouble(stepCalValue.getEndValue());
				if (Double.parseDouble(compare) > start && Double.parseDouble(compare) < end) {
					calValue = stepCalValue.getCalValue();
				}
			}
			if (calType.equals("ratio")) {
				value = Double.parseDouble(compare) * Double.parseDouble(calValue);
			} else if (calType.equals("fixed")) {
				value = Double.parseDouble(calValue);
			} else if (calType.equals("price")) {
				value = Double.parseDouble(compare) * Double.parseDouble(calValue);
			}
		}

		return value;
	}

	

	public synchronized  void dealBill(String policyCode, double value, String tenantId, String batchNo, String objectId,
			long elementId, String billStyle, String billTime,String feeItemId) {
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		String billAll = billClient.get("bill");
		List<StlBillData> dataList = JSON.parseArray(billAll, StlBillData.class);
		if (!contains(policyCode, dataList)) {
			StlBillData stlBillData = new StlBillData();
			stlBillData.setPolicyCode(policyCode);
			stlBillData.setTenantId(tenantId);
			stlBillData.setBatchNo(batchNo);
			stlBillData.setStlObjectId(objectId);
			stlBillData.setStlElementId(elementId);
			stlBillData.setBillStyleSn(billStyle);
			stlBillData.setOrigFee(Float.parseFloat(String.valueOf(value)));
			stlBillData.setFeeItemId(feeItemId);
			stlBillData.setBillId(Long.parseLong(SmcSeqUtil.getRandom()));
			dataList.add(stlBillData);
		} else {
			for (StlBillData stlBillData : dataList) {
				if (stlBillData.getPolicyCode().equals(policyCode)) {
					stlBillData.setOrigFee(stlBillData.getOrigFee() + Float.parseFloat(String.valueOf(value)));
				}
			}
		}
		billClient.set("bill", JSON.toJSONString(dataList));

	}

	boolean contains(String policyCode, List<StlBillData> dataList) {
		boolean flag = false;
		for (StlBillData stlBillData : dataList) {
			if (stlBillData.getPolicyCode().equals(policyCode)) {
				flag = true;
			}
		}
		return flag;
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
	
	
	public void insertBillData(String tableName,String itemTableName) throws Exception
	{
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		String billAll = billClient.get("bill");
		List<StlBillData> dataList = JSON.parseArray(billAll, StlBillData.class);
		for (StlBillData stlBillData : dataList) {
			insert(stlBillData,tableName,itemTableName);	
		}
	}
	
	
	
	public void insert(StlBillData stlBillData,String tableName,String itemTableName) throws Exception
	{
		Connection connection=JdbcProxy.getConnection("");
		 Statement st = null;
		 st = (Statement) connection.createStatement();
		 String sql = "insert into "+tableName+"(bill_id,bill_from,batch_no,tenant_id,policy_code,stl_object_id,"
		 		+ "stl_element_id,stl_element_sn,bill_style_sn,bill_time_sn,bill_start_time,bill_end_time,orig_fee) values("+stlBillData.getBillId()+","+
				 stlBillData.getBillFrom()+","+stlBillData.getBatchNo()+","+stlBillData.getTenantId()+","+stlBillData.getPolicyCode()+","+
		 		stlBillData.getStlObjectId()+","+stlBillData.getStlElementId()+","+stlBillData.getStlElementSn()+","+stlBillData.getBillTimeSn()+","+stlBillData.getBillStartTime()+","+stlBillData.getBillEndTime()+","+stlBillData.getOrigFee()+")";
         System.out.println("sql="+sql);
         int result = st.executeUpdate(sql);
         
         
         String feeItemSql = "insert into "+itemTableName+"(bill_item_id,bill_id,tenant_id,item_type,fee_item_id,total_fee"
 		 		+ " values(,"+stlBillData.getBillId()+","+
 				 stlBillData.getTenantId()+",'aaa',"+
 		 		stlBillData.getFeeItemId()+","+stlBillData.getOrigFee()+")";
         
         int itemresult = st.executeUpdate(feeItemSql);
	}
	
	
	
	public void exportExcel()
	{
		ICacheClient billClient = CacheClientFactory.getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
		String billAll = billClient.get("bill");
		List<StlBillData> dataList = JSON.parseArray(billAll, StlBillData.class);
		
		
		 // 第一步，创建一个webbook，对应一个Excel文件  
        HSSFWorkbook wb = new HSSFWorkbook();  
        for(int i=0;i<dataList.size();i++)
        {
        	StlBillData stlBillData=(StlBillData)dataList.get(i);	
        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet  
        HSSFSheet sheet = wb.createSheet("账单"+i);  
        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short  
        HSSFRow row = sheet.createRow((int) 0);  
        // 第四步，创建单元格，并设置值表头 设置表头居中  
        HSSFCellStyle style = wb.createCellStyle();  
        style.setAlignment(HSSFCellStyle.ALIGN_CENTER); // 创建一个居中格式  
  
        HSSFCell cell = row.createCell((short) 0);  
        cell.setCellValue("结算方");  
        cell.setCellStyle(style); 
        cell=row.createCell((int)1);
        cell.setCellValue(stlBillData.getBillFrom());
        cell.setCellStyle(style); 
        
        cell = row.createCell((short) 2);  
        cell.setCellValue("批次号");  
        cell.setCellStyle(style);  
        
        cell=row.createCell((int)3);
        cell.setCellValue(stlBillData.getBatchNo());
        cell.setCellStyle(style); 
        
        
        HSSFRow row1 = sheet.createRow((int) 1);  
        HSSFCell cell1 = row1.createCell((short) 0);  
        cell1.setCellValue("政策编码");  
        cell1.setCellStyle(style); 
         cell1 = row1.createCell((short)1);  
        cell1.setCellValue(stlBillData.getPolicyCode());  
        cell1.setCellStyle(style); 
        
        
        cell1 = row1.createCell((short) 2);  
        cell1.setCellValue("账期");  
        cell1.setCellStyle(style);
        
        cell1 = row1.createCell((short) 3);  
        cell1.setCellValue(stlBillData.getBillTimeSn());  
        cell1.setCellStyle(style);
        
        
        
        HSSFRow row2 = sheet.createRow((int) 2);  
        HSSFCell cell2 = row2.createCell((short) 0);  
        cell2.setCellValue("开始时间");  
        cell2.setCellStyle(style); 
        
         cell2 = row2.createCell((short) 1);  
        cell2.setCellValue(stlBillData.getBillStartTime());  
        cell2.setCellStyle(style); 
        
        cell2 = row2.createCell((short) 2);  
        cell2.setCellValue("结束时间");  
        cell2.setCellStyle(style);
        
        cell2 = row2.createCell((short) 3);  
        cell2.setCellValue(stlBillData.getBillEndTime());  
        cell2.setCellStyle(style); 
        
        
        HSSFRow row3 = sheet.createRow((int) 3);  
        HSSFCell cell3 = row3.createCell((short) 0);  
        cell3.setCellValue("结算金额(元)");  
        cell3.setCellStyle(style); 
        
        cell3 = row3.createCell((short) 1);  
        cell3.setCellValue(stlBillData.getOrigFee());  
        cell3.setCellStyle(style); 
        
        
        
        HSSFRow row5 = sheet.createRow((int) 5);  
        HSSFCell cell5 = row5.createCell((short) 0);  
        cell5.setCellValue("科目ID");  
        cell5.setCellStyle(style); 
        cell5 = row5.createCell((short) 1);  
        cell5.setCellValue("科目名称");  
        cell5.setCellStyle(style); 
        cell5 = row5.createCell((short) 2);  
        cell5.setCellValue("总金额(元)");  
        cell5.setCellStyle(style); 
        
        
        HSSFRow row6 = sheet.createRow((int) 6);  
        HSSFCell cell6 = row6.createCell((short) 0);  
        cell6.setCellValue(stlBillData.getFeeItemId());  
        cell6.setCellStyle(style); 
        cell6 = row5.createCell((short) 1);  
        cell6.setCellValue("科目名称");  
        cell6.setCellStyle(style); 
        cell6 = row5.createCell((short) 2);  
        cell6.setCellValue(stlBillData.getOrigFee());  
        cell6.setCellStyle(style); 
        
        
        }
  
		
		
	}
	

}
