package com.ai.baas.smc.calculate.topology.core.util;

public final class SmcCacheConstant {
    private SmcCacheConstant() {
    }

    public static final String CACHE_KEY_SPLIT = ".";

    /**
     * 账单项
     */
    public static final String BILL_ITEM = "bill.item";

    /**
     * 详单项
     */
    public static final String BILL_DETAIL_ITEM = "bill.detail.item";
    
    /**
     * 政策列表
     */
    public static final String POLICY_ALL="policy_all";
    
    public static final String POLICY_ITEM_CONDITION="policy_item_condition";
    
    public static final String POLICY_ITEM_PLAN="policy_item_plan";
    
    public static final String POLICY_ITEM="policy_item";

    public static final class TypeCode {
        private TypeCode() {
        }
        public static final String STL_POLICY_ITEM_PLAN = "STL_POLICY_ITEM_PLAN";

        public static final String SFTP_CONF = "SFTP_CONF";

        public static final String AUTH = "AUTH";

        public static final String DATA_COLLECT = "data_collect";
    }

    public static final class ParamCode {
        private ParamCode() {
        }
        public static final String FEE_ITEM = "FEE_ITEM";

        public static final String USER_NAME = "USER_NAME";

        public static final String PWD = "PWD";

        public static final String USER_ID = "USER_ID";

        public static final String PAASWD = "PASSWD";

        public static final String URL = "url";

        public static final String UPLOAD_URL_DIFF_FILE = "upload_url_diff_file";
    }

    public static final class NameSpace {

        private NameSpace() {
        }

        /**
         * sys_param
         */
        public static final String SYS_PARAM_CACHE = "com.ai.baas.smc.cache.sysparam";

        public static final String POLICY_CACHE = "com.ai.baas.smc.cache.policy";

        public static final String BILL_STYLE_CACHE = "com.ai.baas.smc.cache.billstyle";

        public static final String ELEMENT_CACHE = "com.ai.baas.smc.cache.element";
        
        public static final String CAL_COMMON_CACHE="com.ai.topology.calculate.common";
        
        public static final String STATS_TIMES = "stats_times";
        
        public static final String BILL_CACHE="com.ai.topology.bill.cache";
        
    }
    
    
    public static class Cache{
    	public static final String COUNTER = "smc_stat_counter";
    	
    	public static final String finishKey = "smc_stat_times_test";
    	
    	public static final String lockKey = "smc_stat_lock";
    	
    	public static final String BILL_PREFIX = "smc_bill_";
    	
    	public static final String BILL_ITEM_PREFIX = "smc_bill_item_";
    	
    	public static final String BILL_DATA_PREFIX = "smc_bill_data_";
    	
    	public static final String BILL_ITEM_DATA_PREFIX = "smc_bill_item_data_";
    }
    
    

    public static class Dshm {
        public static class TableName {
            public static final String STL_IMPORT_LOG = "stl_import_log";
        }

        public static class FieldName {

            public static final String TENANT_ID = "tenant_id";

            public static final String BATCH_NO = "batch_no";

            public static final Object BILL_TIME_SN = "bill_time_sn";

        }

        public static class OptType {
            public static final int INSERT = 1;

            public static final int UPDATE = 0;
        }
    }
}
