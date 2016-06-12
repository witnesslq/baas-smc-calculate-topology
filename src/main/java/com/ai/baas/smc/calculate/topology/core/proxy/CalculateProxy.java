package com.ai.baas.smc.calculate.topology.core.proxy;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import com.ai.baas.smc.api.policymanage.param.StepCalValue;
import com.ai.baas.smc.calculate.topology.core.bo.StlBillData;
import com.ai.baas.smc.calculate.topology.core.bo.StlBillItemData;
import com.ai.baas.smc.calculate.topology.core.bo.StlElement;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicy;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItem;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemCondition;
import com.ai.baas.smc.calculate.topology.core.bo.StlPolicyItemPlan;
import com.ai.baas.smc.calculate.topology.core.bo.StlSysParam;
import com.ai.baas.smc.calculate.topology.core.bo.SwitchCalValue;
import com.ai.baas.smc.calculate.topology.core.util.DateUtil;
import com.ai.baas.smc.calculate.topology.core.util.IKin;
import com.ai.baas.smc.calculate.topology.core.util.SftpUtil;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant.ParamCode;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheConstant.TypeCode;
import com.ai.baas.smc.calculate.topology.core.util.SmcCacheFactory;
import com.ai.baas.smc.calculate.topology.core.util.SmcConstants;
import com.ai.baas.smc.calculate.topology.core.util.SmcSeqUtil;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.jdbc.JdbcTemplate;
import com.ai.baas.storm.sequence.datasource.SeqDataSourceLoader;
import com.ai.baas.storm.sequence.util.SeqUtil;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.components.mcs.MCSClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mysql.jdbc.Statement;

public class CalculateProxy {
    private static final Logger log = LoggerFactory.getLogger(CalculateProxy.class);

    private String detail_bill_prefix = "stl_bill_detail_data_";

    private String detail_bill_cf = "col_def";

    private String exportLocal = "~/export";

    private int export_max = 50000;

    public CalculateProxy(Map<String, String> stormConf) {
        String localpath = stormConf.get("smc.calculate.export.local.temp");
        if (StringUtils.isNotBlank(localpath)) {
            this.exportLocal = localpath;
        }
        String lineMax = stormConf.get("smc.calculate.export.csv.line.max");
        if (StringUtils.isNotBlank(lineMax)) {
            export_max = Integer.parseInt(lineMax);
        }
        initSeq(stormConf.get(BaseConstants.JDBC_DEFAULT));
    }

    private void initSeq(String jsonJdbcParam) {
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject) jsonParser.parse(jsonJdbcParam);
        Map<String, String> config = new HashMap<String, String>();
        for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {
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
     */
    public List<StlPolicy> getPolicyList(String objectId, String tenantId) throws Exception {
        ICacheClient cacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
        String policyAll = cacheClient.hget(SmcCacheConstant.NameSpace.POLICY_CACHE,
                SmcCacheConstant.POLICY_ALL);
        List<StlPolicy> stlPolicyList = new ArrayList<StlPolicy>();
        List<StlPolicy> policyList = JSON.parseArray(policyAll, StlPolicy.class);
        for (int i = 0; i < policyList.size(); i++) {
            StlPolicy stlPolicy = (StlPolicy) policyList.get(i);
            if (stlPolicy.getStlObjectId().equals(objectId)
                    && stlPolicy.getTenantId().equals(tenantId)) {
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
    public List<StlPolicyItem> getStlPolicyItemLists(Long policyId, String tenantId) {
        ICacheClient cacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
        String policyItemAll = cacheClient.hget(SmcCacheConstant.NameSpace.POLICY_CACHE,
                SmcCacheConstant.POLICY_ITEM);
        List<StlPolicyItem> stlPolicyItemList = new ArrayList<StlPolicyItem>();
        List<StlPolicyItem> policyList = JSON.parseArray(policyItemAll, StlPolicyItem.class);
        for (StlPolicyItem stlPolicyItem : policyList) {
            if (stlPolicyItem.getPolicyId().longValue() == policyId.longValue()
                    && stlPolicyItem.getTenantId().equals(tenantId)) {
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
     */
    public List<StlPolicyItemCondition> getPolicyItemList(Long itemId, String tenantId)
            throws Exception {
        ICacheClient cacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
        String policyItemAll = cacheClient.hget(SmcCacheConstant.NameSpace.POLICY_CACHE,
                SmcCacheConstant.POLICY_ITEM_CONDITION);
        List<StlPolicyItemCondition> stlPolicyItemConditionList = new ArrayList<StlPolicyItemCondition>();
        List<StlPolicyItemCondition> policyList = JSON.parseArray(policyItemAll,
                StlPolicyItemCondition.class);
        for (StlPolicyItemCondition stlPolicyItemCondition : policyList) {
            if (stlPolicyItemCondition.getItemId().longValue() == itemId.longValue()
                    && stlPolicyItemCondition.getTenantId().equals(tenantId)) {
                stlPolicyItemConditionList.add(stlPolicyItemCondition);
            }
        }
        return stlPolicyItemConditionList;
    }

    public List<StlPolicyItemPlan> getStlPolicyItemPlan(Long itemId, String tenantId)
            throws Exception {
        ICacheClient cacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.POLICY_CACHE);
        String policyItemAll = cacheClient.hget(SmcCacheConstant.NameSpace.POLICY_CACHE,
                SmcCacheConstant.POLICY_ITEM_PLAN);
        List<StlPolicyItemPlan> stlPolicyItemPlanList = new ArrayList<StlPolicyItemPlan>();
        List<StlPolicyItemPlan> policyList = JSON
                .parseArray(policyItemAll, StlPolicyItemPlan.class);
        for (int i = 0; i < policyList.size(); i++) {
            StlPolicyItemPlan stlPolicyItemPlan = (StlPolicyItemPlan) policyList.get(i);
            if (stlPolicyItemPlan.getItemId().longValue() == itemId.longValue()
                    && stlPolicyItemPlan.getTenantId().equals(tenantId)) {
                stlPolicyItemPlanList.add(stlPolicyItemPlan);
            }
        }
        return stlPolicyItemPlanList;
    }

    // public List getParamList() {
    // IDshmClient client = null;
    // if (client == null)
    // client = new DshmClient();
    // ICacheClient cacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
    // Map<String, String> params = new TreeMap<String, String>();
    // params.put("price_code", "999");
    // params.put("tenant_id", "VIV-BYD");
    // List<Map<String, String>> results =
    // client.list("cp_price_info").where(params).executeQuery(cacheClient);
    // return results;
    // }

    /**
     * 是否匹配规则
     * 
     * @param stream
     * @param paramList
     * @param policyItemList
     * @return
     */
    public boolean matchPolicy(Map<String, String> data, List<StlPolicyItemCondition> policyItemList) {
        boolean flag = false;
        ICacheClient elementcacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);
        for (StlPolicyItemCondition stlPolicyItemCondition : policyItemList) {
            StringBuilder elementStr = new StringBuilder();
            elementStr.append(stlPolicyItemCondition.getTenantId());
            elementStr.append(".");
            elementStr.append(String.valueOf(stlPolicyItemCondition.getElementId()));
            // String compare =(String)data.get("content");
            String elementJson = elementcacheClient.hget(SmcCacheConstant.NameSpace.ELEMENT_CACHE,
                    elementStr.toString());
            if (StringUtils.isBlank(elementJson)) {
                break;
            }
            StlElement stlElement = JSON.parseObject(elementJson, StlElement.class);
            String compare = "";
            if (stlElement.getAttrType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_ATTR_TYPE_STAT)) {
                if (stlElement.getStatisticsType().equalsIgnoreCase(
                        SmcConstants.STL_ELEMENT_STAT_TYPE_C_CNT)) {
                    int len = StringUtils.defaultString(data.get("content")).length();
                    compare = String.valueOf(len);
                } else {
                    System.out.println("内存中目前没有统计数据，敬请期待。。。");
                    flag = true;
                    continue;
                }
            } else {
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
                if (matchType.equals("=")) {
                    matchType = "==";
                }
                String expression = "a" + matchType + "b";
                List<Variable> variables = new ArrayList<Variable>();
                variables.add(Variable.createVariable("a", compare));
                variables.add(Variable.createVariable("b", matchValue));
                Object result = ExpressionEvaluator.evaluate(expression, variables);
                flag = Boolean.parseBoolean(result.toString());
            }

            // 只要存在条件不满足,就退出
            if (!flag) {
                break;
            }
        }

        // System.out.println("flag====="+flag);
        return flag;
    }

    // public Map valueMap(String[] stream, List paramList) {
    // Map map = new HashMap();
    // for (int i = 0; i < stream.length; i++) {
    // for (int j = 0; j < paramList.size(); j++) {
    // Map paramMap = (Map) paramList.get(j);
    // if (paramMap.containsKey(i)) {
    // map.put(paramMap.get(i), stream[i]);
    // }
    // }
    // }
    // return map;
    //
    // }

    public double caculateFees(StlPolicyItemPlan stlPolicyItemPlan, Map<String, String> data) {
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
    public double docaculate(StlPolicyItemPlan policyDetailQueryPlanInfo, Map<String, String> data) {
        double value = 0;
        ICacheClient elementcacheClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.ELEMENT_CACHE);
        String planType = policyDetailQueryPlanInfo.getPlanType();
        String calType = policyDetailQueryPlanInfo.getCalType();// 算费方式
        Long elementId = policyDetailQueryPlanInfo.getElementId();

        StringBuilder elementStr = new StringBuilder();
        elementStr.append(policyDetailQueryPlanInfo.getTenantId());
        elementStr.append(".");
        elementStr.append(elementId);

        String elementJson = elementcacheClient.hget(SmcCacheConstant.NameSpace.ELEMENT_CACHE,
                elementStr.toString());
        StlElement stlElement = JSON.parseObject(elementJson, StlElement.class);
        // long sortId = stlElement.getSortId();
        // int num = (int) sortId;
        String compare = "";
        if (stlElement.getAttrType().equalsIgnoreCase(SmcConstants.STL_ELEMENT_ATTR_TYPE_STAT)) {
            if (stlElement.getStatisticsType().equalsIgnoreCase(
                    SmcConstants.STL_ELEMENT_STAT_TYPE_C_CNT)) {
                int len = StringUtils.defaultString(data.get("content")).length();
                compare = String.valueOf(len);
            } else {
                System.out.println("内存中目前没有统计数据，敬请期待。。。");
                return value;
            }
        } else {
            String elementCode = stlElement.getElementCode();
            compare = data.get(elementCode);
        }
        // compare =(String)data.get("content");
        if (planType.equals("normal")) {// 标准型
            // 可能出现错误，calValue是否是JSON，如果是需要解析转换后使用
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
            List<StepCalValue> stepCalValues = JSON.parseArray(
                    policyDetailQueryPlanInfo.getCalValue(), StepCalValue.class);
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
                    value = Double.parseDouble(compare) * Double.parseDouble(calValue) * value;
                }

            }
        } else if ("switch".equals(planType)) {// 分档

            List<SwitchCalValue> stepCalValues = JSON.parseArray(
                    policyDetailQueryPlanInfo.getCalValue(), SwitchCalValue.class);
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

    public String dealBill(String policyCode, double value, String tenantId, String batchNo,
            String objectId, long elementId, String billStyle, String billTime, String feeItemId,
            String policyId, String elementSn, String bsn) throws Exception {
        ICacheClient billClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
        // 一级key=SMC_BILL_账期 二级key=租户+批次号+账期+政策+科目..................
        // String billAll = billClient.get(getCacheBillKey(policyId));
        String billKey = assembleCacheKey(SmcCacheConstant.Cache.BILL_PREFIX, bsn);
        String billAll = billClient.hget(billKey, policyId);
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
            // stlBillData.setFeeItemId(feeItemId);
            // stlBillData.setBillId(Long.parseLong(SmcSeqUtil.getRandom()));
            stlBillData.setBillId(SeqUtil.getNewId(SmcConstants.STL_BILL_DATA$BILL_ID$SEQ));
            stlBillData.setBillFrom("sys");
            stlBillData.setBillStartTime(DateUtil.getFirstDay(billTime, "yyyyMM"));
            stlBillData.setBillEndTime(DateUtil.getLastDay(billTime, "yyyyMM"));
            stlBillData.setBillTimeSn(billTime);
            // stlBillData.setItemDatas(new ArrayList<StlBillItemData>());

            billClient.hset(billKey, policyId, JSON.toJSONString(stlBillData));
        } else {
            stlBillData = JSON.parseObject(billAll, StlBillData.class);
        }
        billClient.hincrByFloat(assembleCacheKey(SmcCacheConstant.Cache.BILL_DATA_PREFIX, bsn),
                policyId, value);

        String billItemKey = assembleCacheKey(SmcCacheConstant.Cache.BILL_ITEM_PREFIX, bsn);
        String policyIdAndFeeItemKey = assembleCacheKey(policyId, ":", feeItemId);
        String billItemData = billClient.hget(billItemKey, policyIdAndFeeItemKey);
        StlBillItemData stlBillItemData = null;
        if (StringUtils.isBlank(billItemData)) {
            stlBillItemData = new StlBillItemData();
            stlBillItemData.setBillId(stlBillData.getBillId());
            stlBillItemData.setTenantId(stlBillData.getTenantId());
            stlBillItemData.setItemType("1");
            stlBillItemData.setFeeItemId(feeItemId);
            stlBillItemData.setTotalFee(new Double(0));

            billClient.hset(billItemKey, policyIdAndFeeItemKey, JSON.toJSONString(stlBillItemData));
        }
        billClient.hincrByFloat(
                assembleCacheKey(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX, bsn),
                policyIdAndFeeItemKey, value);

        return stlBillData.getBillId().toString();
    }

    boolean contains(List<StlBillItemData> itemDatas, String feeItemId, double value) {
        for (StlBillItemData stlBillItemData : itemDatas) {
            if (feeItemId.equals(stlBillItemData.getFeeItemId())) {
                // double addup = stlBillItemData.getTotalFee().doubleValue() + value;
                // stlBillItemData.setTotalFee(new Double(addup));
                return true;
            }
        }
        return false;
    }

    // boolean contains(String policyCode, List<StlBillData> dataList) {
    // boolean flag = false;
    // for (StlBillData stlBillData : dataList) {
    // if (stlBillData.getPolicyCode().equals(policyCode)) {
    // flag = true;
    // }
    // }
    // return flag;
    // }

    private String assembleCacheKey(String... params) {
        StringBuilder key = new StringBuilder();
        for (String param : params) {
            key.append(param);
        }
        return key.toString();
    }

    public void outputDetailBill(String period, String row, Map<String, String> data) {
        try {
            TableName tableName = TableName.valueOf(detail_bill_prefix + period);
            Table table = HBaseProxy.getConnection().getTable(tableName);
            Put put = new Put(Bytes.toBytes(row));
            byte[] cf = Bytes.toBytes(detail_bill_cf);
            for (Entry<String, String> entry : data.entrySet()) {
                put.addColumn(cf, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getBillDataId(String policyCode) {
        long billDataId = 0;
        ICacheClient billClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
        String billAll = billClient.get("bill");
        List<StlBillData> dataList = JSON.parseArray(billAll, StlBillData.class);
        for (StlBillData stlBillData : dataList) {
            if (stlBillData.getPolicyCode().equals(policyCode)) {
                billDataId = stlBillData.getBillId();
            }
        }
        return billDataId;
    }

    public void insertBillData(String period, String bsn) throws Exception {
        ICacheClient billClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
        Map<String, String> billMaps = billClient.hgetAll(assembleCacheKey(
                SmcCacheConstant.Cache.BILL_PREFIX, bsn));
        int opt_times = 0;
        StlBillData stlBillData = null;
        // for(String bill:billMaps.values()){
        for (Entry<String, String> entry : billMaps.entrySet()) {
            stlBillData = JSON.parseObject(entry.getValue(), StlBillData.class);
            if (insert(stlBillData, period, entry.getKey(), bsn, billClient)) {
                opt_times++;
            }
            // opt_times = 1;
        }
        if (billMaps.size() == opt_times) {
            // 导出文件
            exportFileAndFtp(bsn);

            billClient.del(SmcCacheConstant.Cache.BILL_PREFIX + bsn);
            billClient.del(SmcCacheConstant.Cache.BILL_ITEM_PREFIX + bsn);
            billClient.del(SmcCacheConstant.Cache.BILL_DATA_PREFIX + bsn);
            billClient.del(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX + bsn);
            billClient.hdel(SmcCacheConstant.Cache.lockKey, bsn);
            billClient.hdel(SmcCacheConstant.Cache.COUNTER, bsn);
        }
    }

    public boolean insert(StlBillData stlBillData, String period, String policyId, String bsn,
            ICacheClient client) {
        Connection conn = null;
        boolean isSucc = false;
        try {
            conn = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT);
            conn.setAutoCommit(false);
            String createTime = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
            Statement statement = (Statement) conn.createStatement();
            String origFee = client.hget(
                    assembleCacheKey(SmcCacheConstant.Cache.BILL_DATA_PREFIX, bsn), policyId);
            // System.out.println("origFee=="+origFee);
            insertBillData(stlBillData, period, createTime, statement, origFee);
            // List<StlBillItemData> itemDatas = stlBillData.getItemDatas();
            List<StlBillItemData> itemDatas = getItemDataByPolicyId(policyId, bsn, client);
            String billId = stlBillData.getBillId().toString();
            String totalFee = "0";
            for (StlBillItemData itemData : itemDatas) {
                totalFee = client.hget(
                        assembleCacheKey(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX, bsn),
                        assembleCacheKey(policyId, ":", itemData.getFeeItemId()));
                // System.out.println("totalFee---"+totalFee);
                insertBillItemData(itemData, billId, period, createTime, statement, totalFee);
            }
            conn.commit();
            isSucc = true;
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return isSucc;
    }

    private List<StlBillItemData> getItemDataByPolicyId(String policyId, String bsn,
            ICacheClient client) {
        List<StlBillItemData> itemDatas = new ArrayList<StlBillItemData>();
        Map<String, String> itemDataMap = client.hgetAll(assembleCacheKey(
                SmcCacheConstant.Cache.BILL_ITEM_PREFIX, bsn));
        String[] splits = null;
        StlBillItemData stlBillItemData = null;
        for (Entry<String, String> entry : itemDataMap.entrySet()) {
            splits = StringUtils.splitPreserveAllTokens(entry.getKey(), ":");
            if (policyId.equals(splits[0])) {
                stlBillItemData = JSON.parseObject(entry.getValue(), StlBillItemData.class);
                itemDatas.add(stlBillItemData);
            }
        }
        return itemDatas;
    }

    private int insertBillItemData(StlBillItemData itemData, String billId, String period,
            String createTime, Statement statement, String totalFee) throws SQLException {
        String billItemId = ObjectUtils.toString(
                SeqUtil.getNewId(SmcConstants.STL_BILL_ITEM_DATA$BILL_ITEM_ID$SEQ),
                SmcSeqUtil.getRandom());
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
        System.out.println("itemSql=" + strSql);
        return statement.executeUpdate(strSql.toString());
    }

    private int insertBillData(StlBillData stlBillData, String period, String createTime,
            Statement statement, String origFee) throws SQLException {

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

        System.out.println("sql=" + strSql);
        return statement.executeUpdate(strSql.toString());
    }

    public void exportFileAndFtp(String bsn) {
        ICacheClient billClient = MCSClientFactory
                .getCacheClient(SmcCacheConstant.NameSpace.BILL_CACHE);
        Map<String, String> billAll = billClient.hgetAll(SmcCacheConstant.Cache.BILL_PREFIX + bsn);
        StlBillData stlBillData = null;
        // for(String bill:billAll.values()){
        String policyId = "", exportPath = "";
        String tenantId = "";
        String batchNo = "";
        for (Entry<String, String> entry : billAll.entrySet()) {
            policyId = entry.getKey();
            stlBillData = JSON.parseObject(entry.getValue(), StlBillData.class);
            tenantId = stlBillData.getTenantId();
            batchNo = stlBillData.getBatchNo();
            try {
                exportPath = exportExcel(stlBillData, policyId, bsn, billClient);
                exportCsv(stlBillData, policyId, exportPath);
                // 不用政策Id导出，用账单ID作为导出id
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String zipFilePath = createZipFile(batchNo);
        System.out.println("压缩文件生成本地路径--->>>" + zipFilePath);
        if (uploadFile(tenantId, zipFilePath, billClient)) {
            updateImportLog(zipFilePath, tenantId, batchNo, billClient);

            System.out.println("正在清理打包文件...");
            String rmPath = StringUtils.substringBeforeLast(zipFilePath, ".zip");
            FileUtils.deleteQuietly(FileUtils.getFile(rmPath));
            FileUtils.getFile(zipFilePath).delete();
        }

    }

    private boolean updateImportLog(String zipFile, String tenantId, String batchNo,
            ICacheClient client) {
        String fileName = FileUtils.getFile(zipFile).getName();
        StlSysParam stlSysParam = getSysParamCache(new String[] { tenantId, TypeCode.SFTP_CONF,
                ParamCode.UPLOAD_URL_DIFF_FILE });
        String url = stlSysParam != null ? stlSysParam.getColumnValue() : "";
        StringBuilder strSql = new StringBuilder("update stl_import_log s ");
        strSql.append("set s.RST_FILE_NAME = ?,s.RST_FILE_URL = ?,");
        strSql.append("s.STATE = '4',s.STATE_DESC='数据处理完成',");
        strSql.append("s.STATE_CHG_TIME = ? ");
        strSql.append("where s.BATCH_NO = ? and s.TENANT_ID=?");

        Object[] params = new Object[5];
        params[0] = fileName;
        params[1] = url;
        params[2] = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        params[3] = batchNo;
        params[4] = tenantId;
        Connection conn = null;
        boolean isSucc = false;
        try {
            conn = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT);
            conn.setAutoCommit(false);
            JdbcTemplate.update(strSql.toString(), conn, params);
            isSucc = true;
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return isSucc;
    }

    private boolean uploadFile(String tenantId, String zipFilePath, ICacheClient billClient) {
        boolean isSucc = false;
        try {
            Map<String, String> sftpCfg = getSftpConfig(tenantId);
            if (!verifySftpConfigParam(sftpCfg)) {
                throw new Exception("读取上传SFTP参数失败:" + sftpCfg.toString());
            }
            System.out.println("sftp config=" + sftpCfg.toString());
            SftpUtil sftpUtil = new SftpUtil(sftpCfg.get("user"), sftpCfg.get("pwd"),
                    sftpCfg.get("host"), "");
            sftpUtil.connect();
            sftpUtil.uploadFile(zipFilePath, sftpCfg.get("remote"));
            sftpUtil.disconnect();
            log.debug("上传数据完成!");
            isSucc = true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("上传文件失败,原因[" + e.getMessage() + "]");
        }
        return isSucc;
    }

    private boolean verifySftpConfigParam(Map<String, String> sftpCfg) {
        if (StringUtils.isNotBlank(sftpCfg.get("host"))
                && StringUtils.isNotBlank(sftpCfg.get("remote"))
                && StringUtils.isNotBlank(sftpCfg.get("user"))
                && StringUtils.isNotBlank(sftpCfg.get("pwd"))) {
            return true;
        } else {
            return false;
        }
    }

    private Map<String, String> getSftpConfig(String tenantId) {
        Map<String, String> config = Maps.newHashMap();
        StlSysParam stlSysParam = getSysParamCache(new String[] { tenantId, TypeCode.SFTP_CONF,
                ParamCode.UPLOAD_URL_DIFF_FILE });
        String url = stlSysParam != null ? stlSysParam.getColumnValue() : "";
        String[] split = url.split(":");
        config.put("host", split[0]);
        config.put("remote", split[1]);
        stlSysParam = getSysParamCache(new String[] { tenantId, TypeCode.SFTP_CONF,
                ParamCode.USER_NAME });
        String user = stlSysParam != null ? stlSysParam.getColumnValue() : "";
        config.put("user", user);
        stlSysParam = getSysParamCache(new String[] { tenantId, TypeCode.SFTP_CONF, ParamCode.PWD });
        String pwd = stlSysParam != null ? stlSysParam.getColumnValue() : "";
        config.put("pwd", pwd);
        return config;
    }

    private StlSysParam getSysParamCache(String[] params) {
        ICacheClient client = SmcCacheFactory.getSysParamCacheClient();
        String sysParamKey = Joiner.on(".").join(params);
        String data = client.hget(SmcCacheConstant.NameSpace.SYS_PARAM_CACHE, sysParamKey);
        if (StringUtils.isBlank(data)) {
            return null;
        }
        return JSON.parseArray(data, StlSysParam.class).get(0);
    }

    // private StlSysParam getSysParamCache(String tenantId,String typeCode,String paramCode,String
    // columnValue,ICacheClient client){
    // String sysParamKey = Joiner.on(".").join(tenantId, typeCode, paramCode, columnValue);
    // String data = client.get(sysParamKey);
    // if(StringUtils.isBlank(data)){
    // return null;
    // }
    // return JSON.parseArray(data, StlSysParam.class).get(0);
    // }

    private String createZipFile(String bsn) {
        String zipFilePath = Joiner.on(File.separator).join(exportLocal, bsn);
        String zipFileName = zipFilePath.concat(".zip");
        FileOutputStream outputStream;
        ZipOutputStream out = null;
        try {
            outputStream = new FileOutputStream(zipFileName);
            out = new ZipOutputStream(new BufferedOutputStream(outputStream));
            createCompressedFile(out, FileUtils.getFile(zipFilePath), "");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(out);
        }
        return zipFileName;
    }

    public String exportCsv(StlBillData stlBillData, String policyId, String exportPath) {
        String period = stlBillData.getBillTimeSn();
        TableName tableName = TableName.valueOf(detail_bill_prefix + period);
        Table table = null;
        ResultScanner scanner = null;
        try {
            // String filePath = createCsvFile(stlBillData,exportPath);
            String rowKeyPrefix = Joiner.on(BaseConstants.COMMON_JOINER).join(
                    stlBillData.getTenantId(), stlBillData.getBillId().toString(), period);
            table = HBaseProxy.getConnection().getTable(tableName);
            Scan scan = new Scan();
            scan.setCaching(300);
            scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(
                    rowKeyPrefix)));
            scanner = table.getScanner(scan);
            // outputCsvFile(stlBillData, exportPath, scanner);
            outputExcelFile(stlBillData, exportPath, scanner);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return "";
    }

    private void outputExcelFile(StlBillData stlBillData, String exportPath, ResultScanner scanner) {
        int count = 0;
        int fileCount = 1;
        String qualifierName = "", colunmValue = "";
        List<String> columnNames = new ArrayList<String>();
        List<String> columnValues = new ArrayList<String>();
        XSSFSheet sheet = null;
        Workbook wb = new XSSFWorkbook();
        for (Result res : scanner) {
            for (KeyValue kv : res.raw()) {
                qualifierName = Bytes.toString(kv.getQualifier());
                if (count == 0) {
                    columnNames.add(qualifierName);
                }
                colunmValue = Bytes.toString(kv.getValue());
                columnValues.add(!qualifierName.equalsIgnoreCase("item_fee") ? colunmValue
                        : formatUnit(colunmValue));
            }
            if (count == 0) {
                sheet = (XSSFSheet) wb.createSheet("详单");
            }
            if (count == 0) {
                for (int i = 0; i < columnNames.size(); i++) {
                    XSSFRow row0 = sheet.createRow(count);// 第n行
                    XSSFCell cell = row0.createCell(i);
                    cell.setCellValue(columnNames.get(i));
                }
                count++;
            } else {
                for (int i = 0; i < columnNames.size(); i++) {
                    XSSFRow row0 = sheet.createRow(count);// 第n行
                    XSSFCell cell = row0.createCell(i);
                    cell.setCellValue(columnNames.get(i));
                }
            }
            if (count == export_max - 1) {
                String fileName = Joiner
                        .on(BaseConstants.COMMON_JOINER)
                        .join(stlBillData.getTenantId(), stlBillData.getStlElementSn(),
                                stlBillData.getPolicyCode(), stlBillData.getBillTimeSn(), "详单",
                                fileCount).concat(".xlsx");
                String filePath = Joiner.on(File.separator).join(exportPath, fileName);
                try {
                    FileOutputStream fileOut = new FileOutputStream(filePath + File.separator
                            + fileName);
                    wb.write(fileOut);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                fileCount++;
                count = 0;
            }
        }
        try {
            wb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputCsvFile(StlBillData stlBillData, String exportPath, ResultScanner scanner) {
        int count = 1;
        int fileCount = 1;
        List<String> columnNames = Lists.newArrayList();
        List<String> columnValues;
        BufferedOutputStream buffer = null;
        String qualifierName = "", colunmValue = "";
        try {
            // buffer = new BufferedOutputStream(new FileOutputStream(filePath,true));
            for (Result res : scanner) {
                if (count == 1) {
                    buffer = createCsvFileStream(stlBillData, exportPath, fileCount);
                    fileCount++;
                }
                columnValues = Lists.newArrayList();
                for (KeyValue kv : res.raw()) {
                    // System.out.println(Bytes.toString(kv.getQualifier())+"="+Bytes.toString(kv.getValue()));
                    qualifierName = Bytes.toString(kv.getQualifier());
                    if (count == 1) {
                        columnNames.add(qualifierName);
                    }
                    colunmValue = Bytes.toString(kv.getValue());
                    columnValues.add(!qualifierName.equalsIgnoreCase("item_fee") ? colunmValue
                            : formatUnit(colunmValue));
                }
                if (count == 1) {
                    System.out.println(Joiner.on(",").join(columnNames));
                    IOUtils.write(Joiner.on(",").join(columnNames).concat(IOUtils.LINE_SEPARATOR),
                            buffer, Charsets.UTF_8);
                }
                IOUtils.write(Joiner.on(",").join(columnValues).concat(IOUtils.LINE_SEPARATOR),
                        buffer, SmcConstants.CHARSET_GBK);
                buffer.flush();
                count++;
                if (count > export_max) {
                    count = 1;
                    columnNames = Lists.newArrayList();
                    IOUtils.closeQuietly(buffer);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(buffer);
        }
    }

    private BufferedOutputStream createCsvFileStream(StlBillData stlBillData, String exportPath,
            int fileCount) throws FileNotFoundException {
        String fileName = Joiner
                .on(BaseConstants.COMMON_JOINER)
                .join(stlBillData.getTenantId(), stlBillData.getStlElementSn(),
                        stlBillData.getPolicyCode(), stlBillData.getBillTimeSn(), "详单", fileCount)
                .concat(".csv");
        String filePath = Joiner.on(File.separator).join(exportPath, fileName);
        return new BufferedOutputStream(new FileOutputStream(filePath, true));
    }

    // private String createCsvFile(StlBillData stlBillData,String exportPath) throws IOException{
    // String fileName = Joiner
    // .on(BaseConstants.COMMON_JOINER)
    // .join(stlBillData.getTenantId(), stlBillData.getPolicyCode(),
    // stlBillData.getBillTimeSn()).concat(".csv");
    //
    // String filePath = Joiner.on(File.separator).join(exportPath, fileName);
    // //FileUtils.forceMkdir(FileUtils.getFile(exportPath));
    // return filePath;
    // }

    public String exportExcel(StlBillData stlBillData, String policyId, String bsn,
            ICacheClient billClient) throws Exception {
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
        cell.setCellValue(stlBillData.getStlElementSn());
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
        cell.setCellValue(DateFormatUtils.format(stlBillData.getBillStartTime().getTime(),
                "yyyy-MM-dd"));
        cell = row2.createCell(2);
        cell.setCellValue("结束时间");
        cell.setCellStyle(cellStyle);
        cell = row2.createCell(3);
        cell.setCellValue(DateFormatUtils.format(stlBillData.getBillEndTime().getTime(),
                "yyyy-MM-dd"));

        XSSFRow row3 = sheet0.createRow(3);// 第四行
        cell = row3.createCell(0);
        cell.setCellValue("结算金额(元)");
        cell.setCellStyle(cellStyle);
        cell = row3.createCell(1);

        String origFee = billClient.hget(
                assembleCacheKey(SmcCacheConstant.Cache.BILL_DATA_PREFIX, bsn), policyId);
        // if (StringUtils.isNotBlank(origFee)) {
        // BigDecimal bOrigFee = new BigDecimal(Double.parseDouble(origFee));
        // origFee = bOrigFee.divide(new BigDecimal(1000), 2,
        // BigDecimal.ROUND_HALF_UP).toPlainString();
        // }else{
        // origFee = "0";
        // }
        cell.setCellValue(formatUnit(origFee));

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
        // List<StlBillItemData> itemDatas = stlBillData.getItemDatas();
        List<StlBillItemData> itemDatas = getItemDataByPolicyId(policyId, bsn, billClient);
        String totalFee = "0";
        String feeItemId = "";
        for (StlBillItemData itemData : itemDatas) {
            XSSFRow rowTmp = sheet0.createRow(lineNo);
            cell = rowTmp.createCell(0);
            feeItemId = itemData.getFeeItemId();
            cell.setCellValue(feeItemId);
            cell = rowTmp.createCell(1);
            // cell.setCellValue("科目名称");
            StlSysParam stlSysParam = getSysParamCache(new String[] { stlBillData.getTenantId(),
                    TypeCode.STL_POLICY_ITEM_PLAN, ParamCode.FEE_ITEM, feeItemId });
            String columnDesc = stlSysParam != null ? stlSysParam.getColumnDesc() : "";
            cell.setCellValue(columnDesc);
            cell = rowTmp.createCell(2);
            totalFee = billClient.hget(
                    assembleCacheKey(SmcCacheConstant.Cache.BILL_ITEM_DATA_PREFIX, bsn),
                    assembleCacheKey(policyId, ":", itemData.getFeeItemId()));
            cell.setCellValue(formatUnit(totalFee));
            lineNo++;
        }

        String fileName = Joiner
                .on(BaseConstants.COMMON_JOINER)
                .join(stlBillData.getTenantId(), stlBillData.getStlElementSn(),
                        stlBillData.getPolicyCode(), stlBillData.getBillTimeSn(),
                        stlBillData.getBillId()).concat(".xlsx");

        String local = Joiner.on(File.separator).join(exportLocal, stlBillData.getBatchNo(),
                policyId);

        FileUtils.forceMkdir(FileUtils.getFile(local.toString()));

        FileOutputStream fileOut = new FileOutputStream(local + File.separator + fileName);
        wb.write(fileOut);

        return local.toString();
    }

    private void createCompressedFile(ZipOutputStream out, File file, String dir) {
        try {
            // 如果当前的是文件夹，则进行进一步处理
            if (file.isDirectory()) {
                // 得到文件列表信息
                File[] files = file.listFiles();
                // 将文件夹添加到下一级打包目录
                out.putNextEntry(new ZipEntry(dir + File.separator));
                dir = dir.length() == 0 ? "" : dir + File.separator;
                // 循环将文件夹中的文件打包
                for (int i = 0; i < files.length; i++) {
                    createCompressedFile(out, files[i], dir + files[i].getName()); // 递归处理
                }
            } else { // 当前的是文件，打包处理
                // 文件输入流
                FileInputStream fis = new FileInputStream(file);
                out.putNextEntry(new ZipEntry(dir));
                // 进行写操作
                int j = 0;
                byte[] buffer = new byte[1024];
                while ((j = fis.read(buffer)) > 0) {
                    out.write(buffer, 0, j);
                }
                // 关闭输入流
                fis.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    private String formatUnit(String fee) {
        String formatFee = "0";
        if (StringUtils.isNotBlank(fee)) {
            BigDecimal bOrigFee = new BigDecimal(Double.parseDouble(fee));
            formatFee = bOrigFee.divide(new BigDecimal(1000), 2, BigDecimal.ROUND_HALF_UP)
                    .toPlainString();
        }
        return formatFee;
    }

}
