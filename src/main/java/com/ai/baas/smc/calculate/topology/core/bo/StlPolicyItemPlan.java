package com.ai.baas.smc.calculate.topology.core.bo;

import java.sql.Timestamp;

public class StlPolicyItemPlan {
    private Long planId;

    private String tenantId;

    private Long itemId;

    private Long policyId;

    private String objectId;

    private Long elementId;

    private String feeItem;

    private String planType;

    private String calType;

    private String calValue;

    private String updateDeptId;

    private String updateOperId;

    private Timestamp updateTime;

    public Long getPlanId() {
        return planId;
    }

    public void setPlanId(Long planId) {
        this.planId = planId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId == null ? null : tenantId.trim();
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId == null ? null : objectId.trim();
    }

    public Long getElementId() {
        return elementId;
    }

    public void setElementId(Long elementId) {
        this.elementId = elementId;
    }

    public String getFeeItem() {
        return feeItem;
    }

    public void setFeeItem(String feeItem) {
        this.feeItem = feeItem == null ? null : feeItem.trim();
    }

    public String getPlanType() {
        return planType;
    }

    public void setPlanType(String planType) {
        this.planType = planType == null ? null : planType.trim();
    }

    public String getCalType() {
        return calType;
    }

    public void setCalType(String calType) {
        this.calType = calType == null ? null : calType.trim();
    }

    public String getCalValue() {
        return calValue;
    }

    public void setCalValue(String calValue) {
        this.calValue = calValue == null ? null : calValue.trim();
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
}