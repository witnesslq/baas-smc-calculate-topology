package com.ai.baas.smc.calculate.topology.core.bo;

import java.io.Serializable;

public class SwitchCalValue implements Serializable {

	private static final long serialVersionUID = -6920678513699572195L;
	private String start_value;
	private String end_value;
	private String value;

	public String getStart_value() {
		return start_value;
	}

	public void setStart_value(String start_value) {
		this.start_value = start_value;
	}

	public String getEnd_value() {
		return end_value;
	}

	public void setEnd_value(String end_value) {
		this.end_value = end_value;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
