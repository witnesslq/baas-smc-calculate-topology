package com.ai.baas.smc.calculate.topology.core.bo;

import java.io.Serializable;

public class StepCalValue implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 起始值
     */
    private String startValue;

    /**
     * 结束值
     */
    private String endValue;

    /**
     * 费率取值
     */
    private String calValue;

    public String getStartValue() {
        return startValue;
    }

    public void setStartValue(String startValue) {
        this.startValue = startValue;
    }

    public String getEndValue() {
        return endValue;
    }

    public void setEndValue(String endValue) {
        this.endValue = endValue;
    }

    public String getCalValue() {
        return calValue;
    }

    public void setCalValue(String calValue) {
        this.calValue = calValue;
    }
}
