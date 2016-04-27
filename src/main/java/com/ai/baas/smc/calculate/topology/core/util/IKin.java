package com.ai.baas.smc.calculate.topology.core.util;

import java.util.List;

import org.apache.storm.guava.collect.Lists;

/**
 * 判断表达式是否包含
 * 
 * @author zhangbc
 *
 */
public  class IKin {

	public static boolean in(String a, String b) {
//		if (b.startsWith("{")) {
//			b = b.substring(1, b.length() - 1);
//		}
		String[] bs = b.split(",");
//		for (int i = 0; i < bs.length; i++) {
//			if (bs[i].equals(a)) {
//				return true;
//			}
//		}
		List<String> data = Lists.newArrayList(bs);
		if(data.contains(b)){
			return true;
		}else{
			return false;
		}
		//return false;
	}

}