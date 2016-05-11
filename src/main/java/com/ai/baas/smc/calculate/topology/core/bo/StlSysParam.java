package com.ai.baas.smc.calculate.topology.core.bo;

import java.sql.Timestamp;

public class StlSysParam {
	private String guidkey;

	private String tenantId;

	private String typeCode;

	private String paramCode;

	private String columnValue;

	private String columnDesc;

	private String subParamCode;

	private String parentValue;

	private Integer dispord;

	private String descb;

	private String state;

	private String updateDeptId;

	private String updateOperId;

	private Timestamp updateTime;

	public String getGuidkey() {
		return guidkey;
	}

	public void setGuidkey(String guidkey) {
		this.guidkey = guidkey;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getTypeCode() {
		return typeCode;
	}

	public void setTypeCode(String typeCode) {
		this.typeCode = typeCode;
	}

	public String getParamCode() {
		return paramCode;
	}

	public void setParamCode(String paramCode) {
		this.paramCode = paramCode;
	}

	public String getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(String columnValue) {
		this.columnValue = columnValue;
	}

	public String getColumnDesc() {
		return columnDesc;
	}

	public void setColumnDesc(String columnDesc) {
		this.columnDesc = columnDesc;
	}

	public String getSubParamCode() {
		return subParamCode;
	}

	public void setSubParamCode(String subParamCode) {
		this.subParamCode = subParamCode;
	}

	public String getParentValue() {
		return parentValue;
	}

	public void setParentValue(String parentValue) {
		this.parentValue = parentValue;
	}

	public Integer getDispord() {
		return dispord;
	}

	public void setDispord(Integer dispord) {
		this.dispord = dispord;
	}

	public String getDescb() {
		return descb;
	}

	public void setDescb(String descb) {
		this.descb = descb;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getUpdateDeptId() {
		return updateDeptId;
	}

	public void setUpdateDeptId(String updateDeptId) {
		this.updateDeptId = updateDeptId;
	}

	public String getUpdateOperId() {
		return updateOperId;
	}

	public void setUpdateOperId(String updateOperId) {
		this.updateOperId = updateOperId;
	}

	public Timestamp getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Timestamp updateTime) {
		this.updateTime = updateTime;
	}

}
