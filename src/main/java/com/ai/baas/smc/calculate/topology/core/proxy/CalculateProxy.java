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
import com.ai.baas.smc.api.policymanage.interfaces.IPolicyManageSV;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryConditionInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryItemInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryPlanInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryRequest;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryResponse;
import com.ai.baas.smc.api.policymanage.param.PolicyListQueryRequest;
import com.ai.baas.smc.api.policymanage.param.PolicyListQueryResponse;
import com.ai.baas.smc.api.policymanage.param.StepCalValue;
import com.ai.baas.smc.calculate.topology.core.util.CacheBLMapper;
import com.ai.baas.smc.calculate.topology.core.util.IKin;
import com.ai.opt.sdk.util.DubboConsumerFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;

/**
 * 
 * @author zhangbc
 *
 */
public class CalculateProxy {

	/**
	 * 根据对象类型获取该对象下有效政策
	 * 
	 * @param objectId
	 * @param tenantId
	 * @return
	 */
	public List getPolicyList(String objectId, String tenantId) {
		// TODO Auto-generated method stub
		IPolicyManageSV policyManageSV = DubboConsumerFactory.getService(IPolicyManageSV.class);
		PolicyListQueryRequest policyListQueryRequest = new PolicyListQueryRequest();
		policyListQueryRequest.setTenantId(tenantId);
		policyListQueryRequest.setDataObjectId(objectId);
		PolicyListQueryResponse response = policyManageSV.queryPolicyList(policyListQueryRequest);
		List list = response.getPageInfo().getResult();
		return list;
	}

	/**
	 * 获取政策下面的所有结算项
	 * 
	 * @param policyId
	 * @param tenantId
	 * @return
	 */
	public List<PolicyDetailQueryItemInfo> getPolicyItemList(Long policyId, String tenantId) {
		// TODO Auto-generated method stub
		IPolicyManageSV policyManageSV = DubboConsumerFactory.getService(IPolicyManageSV.class);
		PolicyDetailQueryRequest request = new PolicyDetailQueryRequest();
		request.setPolicyId(policyId);
		request.setTenantId(tenantId);
		PolicyDetailQueryResponse response = policyManageSV.queryPolicyDetail(request);
		return response.getPolicyDetailQueryItemInfos();
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
	public boolean matchPolicy(String[] stream, List paramList, List<PolicyDetailQueryItemInfo> policyItemList) {
		Map map = valueMap(stream, paramList);

		boolean flag = true;
		IElementManageSV elementManageSV = DubboConsumerFactory.getService(IElementManageSV.class);
		for (PolicyDetailQueryItemInfo policyDetailQueryItemInfo : policyItemList) {
			List<PolicyDetailQueryConditionInfo> policyConditionList = policyDetailQueryItemInfo
					.getPolicyDetailQueryConditionInfos();
			for (PolicyDetailQueryConditionInfo policyDetailQueryConditionInfo : policyConditionList) {
				String matchType = policyDetailQueryConditionInfo.getMatchType();
				String matchValue = policyDetailQueryConditionInfo.getMatchValue();
				Long elementId = policyDetailQueryConditionInfo.getElementId();
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

	public void caculateFees(List<PolicyDetailQueryItemInfo> list, String[] stream) {
		for (PolicyDetailQueryItemInfo policyDetailQueryItemInfo : list) {
			List<PolicyDetailQueryPlanInfo> planInfoList = policyDetailQueryItemInfo.getPolicyDetailQueryPlanInfos();
			for (PolicyDetailQueryPlanInfo policyDetailQueryPlanInfo : planInfoList) {

			}
		}
	}

	/**
	 * 算费
	 * 
	 * @param planType
	 * @param calType
	 * @return
	 */
	public double docaculate(PolicyDetailQueryPlanInfo policyDetailQueryPlanInfo, String[] stream) {
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
			double calValue=Double.parseDouble(policyDetailQueryPlanInfo.getNormalCalValue());
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
			List<StepCalValue> list = policyDetailQueryPlanInfo.getStepCalValues();
			String calValue = "";
			for (StepCalValue stepCalValue : list) {
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

			List<StepCalValue> list = policyDetailQueryPlanInfo.getStepCalValues();
			String calValue = "";
			for (StepCalValue stepCalValue : list) {
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
