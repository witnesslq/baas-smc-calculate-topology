package com.ai.baas.smc.calculate.topology.core.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;

public class RadisUtil {
	
	
	public static String getValueByparam(String param)
	{
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
		if(results.size()>0)
		{
			return results.get(0).get(param);
		}
		else
		{
			return null;
		}
	}

}
