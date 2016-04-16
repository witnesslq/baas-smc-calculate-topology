package com.ai.baas.smc.calculate.topology.core.util;

import java.util.Random;


public final class SmcSeqUtil {

    
    public static final String numberChar = "0123456789";
    
    public static String getRandom() {
    	 
        Long seed = System.currentTimeMillis();// 获得系统时间，作为生成随机数的种子
 
        StringBuffer sb = new StringBuffer();// 装载生成的随机数
 
        Random random = new Random(seed);// 调用种子生成随机数
 
        for (int i = 0; i < 10; i++) {
 
            sb.append(numberChar.charAt(random.nextInt(numberChar.length())));
        }
 
        return sb.toString();
 
    }
    
    
    public static void main(String [] args)
    {
    	System.out.println(Long.parseLong(getRandom()));
    }
    
}
