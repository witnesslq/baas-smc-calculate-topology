package com.ai.baas.smc.calculate.topology.core.bo;

import java.sql.Timestamp;

public class StlPolicy {
    private Long policyId;

    private String tenantId;

    private String policyCode;

    private String policyName;

    private Timestamp startTime;

    private Timestamp endTime;

    private String policyType;

    private String policyDesc;

    private String execCycle;

    private String execTimeStr;

    private String dataObjectId;

    private Long dataElementId;

    private String stlObjectId;

    private Long stlElementId;
    
    private String stlElementSn;

    private String billStyleSn;

    private String checkFeeFlag;

    private String state;

    private String createDeptId;

    private String createOperId;

    private Timestamp createTime;

    private String updateDeptId;

    private String updateOperId;

    private Timestamp updateTime;

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId == null ? null : tenantId.trim();
    }

    public String getPolicyCode() {
        return policyCode;
    }

    public void setPolicyCode(String policyCode) {
        this.policyCode = policyCode == null ? null : policyCode.trim();
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName == null ? null : policyName.trim();
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public String getPolicyType() {
        return policyType;
    }

    public void setPolicyType(String policyType) {
        this.policyType = policyType == null ? null : policyType.trim();
    }

    public String getPolicyDesc() {
        return policyDesc;
    }

    public void setPolicyDesc(String policyDesc) {
        this.policyDesc = policyDesc == null ? null : policyDesc.trim();
    }

    public String getExecCycle() {
        return execCycle;
    }

    public void setExecCycle(String execCycle) {
        this.execCycle = execCycle == null ? null : execCycle.trim();
    }

    public String getExecTimeStr() {
        return execTimeStr;
    }

    public void setExecTimeStr(String execTimeStr) {
        this.execTimeStr = execTimeStr == null ? null : execTimeStr.trim();
    }

    public String getDataObjectId() {
        return dataObjectId;
    }

    public void setDataObjectId(String dataObjectId) {
        this.dataObjectId = dataObjectId == null ? null : dataObjectId.trim();
    }

    public Long getDataElementId() {
        return dataElementId;
    }

    public void setDataElementId(Long dataElementId) {
        this.dataElementId = dataElementId;
    }

    public String getStlObjectId() {
        return stlObjectId;
    }

    public void setStlObjectId(String stlObjectId) {
        this.stlObjectId = stlObjectId == null ? null : stlObjectId.trim();
    }

    public Long getStlElementId() {
        return stlElementId;
    }

    public void setStlElementId(Long stlElementId) {
        this.stlElementId = stlElementId;
    }

    public String getBillStyleSn() {
        return billStyleSn;
    }

    public void setBillStyleSn(String billStyleSn) {
        this.billStyleSn = billStyleSn == null ? null : billStyleSn.trim();
    }

    public String getCheckFeeFlag() {
        return checkFeeFlag;
    }

    public void setCheckFeeFlag(String checkFeeFlag) {
        this.checkFeeFlag = checkFeeFlag == null ? null : checkFeeFlag.trim();
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state == null ? null : state.trim();
    }

    public String getCreateDeptId() {
        return createDeptId;
    }

    public void setCreateDeptId(String createDeptId) {
        this.createDeptId = createDeptId == null ? null : createDeptId.trim();
    }

    public String getCreateOperId() {
        return createOperId;
    }

    public void setCreateOperId(String createOperId) {
        this.createOperId = createOperId == null ? null : createOperId.trim();
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getUpdateDeptId() {
        return updateDeptId;
    }

    public void setUpdateDeptId(String updateDeptId) {
        this.updateDeptId = updateDeptId == null ? null : updateDeptId.trim();
    }

    public String getUpdateOperId() {
        return updateOperId;
    }

    public void setUpdateOperId(String updateOperId) {
        this.updateOperId = updateOperId == null ? null : updateOperId.trim();
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }

	public String getStlElementSn() {
		return stlElementSn;
	}

	public void setStlElementSn(String stlElementSn) {
		this.stlElementSn = stlElementSn;
	}
    
    
    
}