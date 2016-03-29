package com.ai.baas.smc.calculate.topology.core.service;

import java.util.List;

import com.ai.baas.smc.api.policymanage.param.PolicyDetailQueryItemInfo;

/**
 * 
 * @author zhangbc
 *
 */
public interface ICalculateService {
	
	
	List getPolicyList(String objectId,String tenantId);
	
	
	List<PolicyDetailQueryItemInfo> getPolicyItemList(Long policyId,String tenantId);
	
	/**
	 * 获取缓存中的流水的顺序信息
	 * @return
	 */
	List getParamList();
	
	
	boolean matchPolicy(String [] stream,List paramList,List<PolicyDetailQueryItemInfo> policyItemList);
	

}
