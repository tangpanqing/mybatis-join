package com.github.tangpanqing.mybatisjoin.entity;

public class JoinWhereItem {
    String fieldName;
    String optName;
    Object val;

    public JoinWhereItem(String fieldName, String optName, Object val) {
        this.fieldName = fieldName;
        this.optName = optName;
        this.val = val;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getOptName() {
        return optName;
    }

    public void setOptName(String optName) {
        this.optName = optName;
    }

    public Object getVal() {
        return val;
    }

    public void setVal(Object val) {
        this.val = val;
    }
}