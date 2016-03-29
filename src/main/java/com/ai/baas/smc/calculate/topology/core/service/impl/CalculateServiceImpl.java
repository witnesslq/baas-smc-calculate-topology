package com.ai.baas.smc.calculate.topology.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.api.policymanage.interfaces.IPolicyManageSV;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryConditionInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryItemInfo;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryRequest;
import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryResponse;
import com.ai.baas.smc.api.policymanage.param.PolicyListQueryRequest;
import com.ai.baas.smc.api.policymanage.param.PolicyListQueryResponse;
import com.ai.baas.smc.calculate.topology.core.service.ICalculateService;
import com.ai.baas.smc.calculate.topology.core.util.CacheBLMapper;
import com.ai.opt.sdk.util.DubboConsumerFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
/**
 * 
 * @author zhangbc
 *
 */
public class CalculateServiceImpl implements ICalculateService {

	@Override
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

	@Override
	public List<PolicyDetailQueryItemInfo> getPolicyItemList(Long policyId, String tenantId) {
		// TODO Auto-generated method stub
		IPolicyManageSV policyManageSV = DubboConsumerFactory.getService(IPolicyManageSV.class);
		PolicyDetailQueryRequest request = new PolicyDetailQueryRequest();
		request.setPolicyId(policyId);
		request.setTenantId(tenantId);
		PolicyDetailQueryResponse response = policyManageSV.queryPolicyDetail(request);
		return response.getPolicyDetailQueryItemInfos();
	}

	@Override
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

	@Override
	public boolean matchPolicy(String[] stream, List paramList, List<PolicyDetailQueryItemInfo> policyItemList) {
		// TODO Auto-generated method stub
		List valueList=valueList(stream,paramList);
		for(PolicyDetailQueryItemInfo policyDetailQueryItemInfo:policyItemList)
		{
		 List<PolicyDetailQueryConditionInfo> policyConditionList=policyDetailQueryItemInfo.getPolicyDetailQueryConditionInfos();
		 for(PolicyDetailQueryConditionInfo policyDetailQueryConditionInfo:policyConditionList)
		 {
//			if()
//			{
//				return false;
//			}
		 }
		}
		
		return true;
	}
	
	
	
	public List valueList(String [] stream,List paramList)
	{
		List<Map<String,String>> valueList=new ArrayList<Map<String,String>>();
		for(int i=0;i<stream.length;i++)
		{
			Map map=new HashMap();
			for(int j=0;j<paramList.size();j++)
			{
				Map paramMap=(Map) paramList.get(j);
				if(paramMap.containsKey(i))
				{
					map.put(paramMap.get(i), stream[i])	;
				}
			}
			valueList.add(map);
		}
		return valueList;
		
	}
	

}
