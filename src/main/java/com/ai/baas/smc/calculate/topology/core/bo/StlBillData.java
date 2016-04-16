package com.ai.baas.smc.calculate.topology.core.bo;

import java.sql.Timestamp;

public class StlBillData {
    private Long billId;

    private String billFrom;

    private String batchNo;

    private String tenantId;

    private String policyCode;

    private String stlObjectId;

    private Long stlElementId;

    private String stlElementSn;

    private String billStyleSn;

    private String billTimeSn;

    private Timestamp billStartTime;

    private Timestamp billEndTime;

    private Float origFee;

    private String checkState;

    private Float diffFee;

    private String checkStateDesc;

    private Timestamp checkTime;

    private Float adjustFee;

    private Float finalFee;

    private Timestamp adjustTime;

    private String adjustOperId;

    private String adjustDesc;

    private String extendStr;

    private String createDeptId;

    private String createOperId;

    private Timestamp createTime;

    private String yyyyMm;

    public Long getBillId() {
        return billId;
    }

    public void setBillId(Long billId) {
        this.billId = billId;
    }

    public String getBillFrom() {
        return billFrom;
    }

    public void setBillFrom(String billFrom) {
        this.billFrom = billFrom == null ? null : billFrom.trim();
    }

    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo == null ? null : batchNo.trim();
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

    public String getStlElementSn() {
        return stlElementSn;
    }

    public void setStlElementSn(String stlElementSn) {
        this.stlElementSn = stlElementSn == null ? null : stlElementSn.trim();
    }

    public String getBillStyleSn() {
        return billStyleSn;
    }

    public void setBillStyleSn(String billStyleSn) {
        this.billStyleSn = billStyleSn == null ? null : billStyleSn.trim();
    }

    public String getBillTimeSn() {
        return billTimeSn;
    }

    public void setBillTimeSn(String billTimeSn) {
        this.billTimeSn = billTimeSn == null ? null : billTimeSn.trim();
    }

    public Timestamp getBillStartTime() {
        return billStartTime;
    }

    public void setBillStartTime(Timestamp billStartTime) {
        this.billStartTime = billStartTime;
    }

    public Timestamp getBillEndTime() {
        return billEndTime;
    }

    public void setBillEndTime(Timestamp billEndTime) {
        this.billEndTime = billEndTime;
    }

    public Float getOrigFee() {
        return origFee;
    }

    public void setOrigFee(Float origFee) {
        this.origFee = origFee;
    }

    public String getCheckState() {
        return checkState;
    }

    public void setCheckState(String checkState) {
        this.checkState = checkState == null ? null : checkState.trim();
    }

    public Float getDiffFee() {
        return diffFee;
    }

    public void setDiffFee(Float diffFee) {
        this.diffFee = diffFee;
    }

    public String getCheckStateDesc() {
        return checkStateDesc;
    }

    public void setCheckStateDesc(String checkStateDesc) {
        this.checkStateDesc = checkStateDesc == null ? null : checkStateDesc.trim();
    }

    public Timestamp getCheckTime() {
        return checkTime;
    }

    public void setCheckTime(Timestamp checkTime) {
        this.checkTime = checkTime;
    }

    public Float getAdjustFee() {
        return adjustFee;
    }

    public void setAdjustFee(Float adjustFee) {
        this.adjustFee = adjustFee;
    }

    public Float getFinalFee() {
        return finalFee;
    }

    public void setFinalFee(Float finalFee) {
        this.finalFee = finalFee;
    }

    public Timestamp getAdjustTime() {
        return adjustTime;
    }

    public void setAdjustTime(Timestamp adjustTime) {
        this.adjustTime = adjustTime;
    }

    public String getAdjustOperId() {
        return adjustOperId;
    }

    public void setAdjustOperId(String adjustOperId) {
        this.adjustOperId = adjustOperId == null ? null : adjustOperId.trim();
    }

    public String getAdjustDesc() {
        return adjustDesc;
    }

    public void setAdjustDesc(String adjustDesc) {
        this.adjustDesc = adjustDesc == null ? null : adjustDesc.trim();
    }

    public String getExtendStr() {
        return extendStr;
    }

    public void setExtendStr(String extendStr) {
        this.extendStr = extendStr == null ? null : extendStr.trim();
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

    public String getYyyyMm() {
        return yyyyMm;
    }

    public void setYyyyMm(String yyyyMm) {
        this.yyyyMm = yyyyMm;
    }
}