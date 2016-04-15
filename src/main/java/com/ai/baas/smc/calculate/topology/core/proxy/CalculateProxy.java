package com.ai.baas.smc.calculate.topology.core.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.api.elementmanage.interfaces.IElementManageSV;
import com.ai.baas.smc.api.elementmanage.param.ElementSearchResponseVO;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryItemInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryPlanInfo;
import com.ai.baas.smc.api.policymanage.param.StepCalValue;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.util.CacheBLMapper;
import com.ai.baas.smc.calculate.topology.core.util.IKin;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.util.DubboConsumerFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.dubbo.common.json.ParseException;
import com.alibaba.fastjson.JSON;

/**
 * 
 * @author zhangbc
 *
 */
public class CalculateProxy {
	
	
	public String cal(String[] stream)
	{
		
		
		
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
		ICacheClient cacheClient = CacheClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyAll=cacheClient.get(SmcCacheConstant.POLICY_ALL);
		List<StlPolicy> stlPolicyList=new ArrayList<StlPolicy>();
		List<StlPolicy> policyList=JSON.parseArray(policyAll, StlPolicy.class);
		for(int i=0;i<policyList.size();i++)
		{
			StlPolicy stlPolicy=(StlPolicy)policyList.get(i);
			if(stlPolicy.getStlObjectId().equals(objectId)&&stlPolicy.getTenantId().equals(tenantId))
			{
				stlPolicyList.add(stlPolicy);
			}
		}
		return stlPolicyList;
	}

	/**
	 * 获取政策适配对象
	 * 
	 * @param policyId
	 * @param tenantId
	 * @return
	 * @throws ParseException 
	 */
	public List<StlPolicyItemCondition> getPolicyItemList(Long policyId, String tenantId) throws ParseException {
		// TODO Auto-generated method stub
		ICacheClient cacheClient = CacheClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll=cacheClient.get(SmcCacheConstant.POLICY_ITEM_CONDITION);
		List<StlPolicyItemCondition> stlPolicyItemConditionList=new ArrayList<StlPolicyItemCondition>();
		List<StlPolicyItemCondition> policyList=JSON.parseArray(policyItemAll,StlPolicyItemCondition.class);
		for(StlPolicyItemCondition stlPolicyItemCondition:policyList)
		{
			if(stlPolicyItemCondition.getPolicyId()==policyId&&stlPolicyItemCondition.getTenantId().equals(tenantId))
			{
				stlPolicyItemConditionList.add(stlPolicyItemCondition);
			}
		}
		return stlPolicyItemConditionList;
	}
	
	
	public List<StlPolicyItemPlan> getStlPolicyItemPlan(Long policyId,String tenantId) throws Exception
	{
		ICacheClient cacheClient = CacheClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
		String policyItemAll=cacheClient.get(SmcCacheConstant.POLICY_ITEM_PLAN);
		List<StlPolicyItemPlan> stlPolicyItemPlanList=new ArrayList<StlPolicyItemPlan>();
		List<StlPolicyItemPlan> policyList=JSON.parseArray(policyItemAll, StlPolicyItemPlan.class);
		for(int i=0;i<policyList.size();i++)
		{
			StlPolicyItemPlan stlPolicyItemPlan=(StlPolicyItemPlan)policyList.get(i);
			if(stlPolicyItemPlan.getPolicyId()==policyId&&stlPolicyItemPlan.getTenantId().equals(tenantId))
			{
				stlPolicyItemPlanList.add(stlPolicyItemPlan);
			}
		}
		return stlPolicyItemPlanList;
	}
	

	public List getParamList() {
		IDshmClient client = null;
		if (client == null)
			client = new DshmClient();
		ICacheClient cacheClient = CacheFactoryUtil
				.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
		Map<String, String> params = new TreeMap<String, String>();
		params.put("price_code", "999");
		params.put("tenant_id", "VIV-BYD");
		List<Map<String, String>> results = client.list("cp_price_info")
				.where(params)
				.executeQuery(cacheClient);
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
	public boolean matchPolicy(String[] stream, List<StlPolicyItemCondition> policyItemList) {

		boolean flag = true;
		IElementManageSV elementManageSV = DubboConsumerFactory.getService(IElementManageSV.class);
		
			for (StlPolicyItemCondition stlPolicyItemCondition : policyItemList) {
				String matchType = stlPolicyItemCondition.getMatchType();
				String matchValue = stlPolicyItemCondition.getMatchValue();
				Long elementId = stlPolicyItemCondition.getElementId();
				ElementSearchResponseVO elementSearchResponseVO = elementManageSV.searchElementById(elementId);
				long sortId = elementSearchResponseVO.getSortId();
				int num = (int) sortId;
				String compare = stream[num];

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

	public double caculateFees(List<StlPolicyItemPlan> list, String[] stream) {
		double value=0;
		for (StlPolicyItemPlan stlPolicyItemPlan : list) {
               value=docaculate(stlPolicyItemPlan,stream);
              
			}
		return value;
	}

	/**
	 * 算费
	 * @param planType
	 * @param calType
	 * @return
	 */
	public double docaculate(StlPolicyItemPlan policyDetailQueryPlanInfo, String[] stream) {
		double value = 0;
		IElementManageSV elementManageSV = DubboConsumerFactory.getService(IElementManageSV.class);
		String planType = policyDetailQueryPlanInfo.getPlanType();
		String calType = policyDetailQueryPlanInfo.getCalType();// 算费方式
		Long elementId = policyDetailQueryPlanInfo.getElementId();
		
		ElementSearchResponseVO elementSearchResponseVO = elementManageSV.searchElementById(elementId);
		long sortId = elementSearchResponseVO.getSortId();
		int num = (int) sortId;
		String compare = stream[num];
		if (planType.equals("nomal")) {//标准型
			double calValue=Double.parseDouble(policyDetailQueryPlanInfo.getCalValue());
			if(calType.equals("ratio"))//按比例
			{
				value=Double.parseDouble(compare)*calValue;	
			}
			else if(calType.equals("fixed"))//固定值
			{
				value=calValue;
			}
			else if(calType.equals("price"))//单价
			{
				value=Double.parseDouble(compare)*calValue;		
			}
			
		} else if (planType.equals("step")) {//阶梯
			  List<StepCalValue> stepCalValues = JSON.parseArray(
					  policyDetailQueryPlanInfo.getCalValue(), StepCalValue.class);
			String calValue = "";
			for (StepCalValue stepCalValue : stepCalValues) {
				double start = Double.parseDouble(stepCalValue.getStartValue());
				double end = Double.parseDouble(stepCalValue.getEndValue());
				if(end<value&&start<value)
				{
					calValue = stepCalValue.getCalValue();
					value+=Double.parseDouble(calValue)*(end-start);	
				}
				if (value > start && value < end) {
					calValue = stepCalValue.getCalValue();
					value+=Double.parseDouble(calValue)*(Double.parseDouble(compare)-start);
				}
				
			}
		} else if ("switch".equals(planType)) {//分档

			 List<StepCalValue> stepCalValues = JSON.parseArray(
					  policyDetailQueryPlanInfo.getCalValue(), StepCalValue.class);
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

}
