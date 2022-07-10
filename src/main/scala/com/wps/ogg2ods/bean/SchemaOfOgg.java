package com.wps.ogg2ods.bean;

import com.alibaba.fastjson.JSONObject;

public class SchemaOfOgg {
    private String table;
    private String op_type;
    private String op_ts;
    private String current_ts;
    private String pos;
    private JSONObject before;
    private JSONObject after;
    private String nation_code;
    private String nation_name;

    @Override
    public String toString() {
        return "SchemaOfOgg{" +
                "table='" + table + '\'' +
                ", op_type='" + op_type + '\'' +
                ", op_ts='" + op_ts + '\'' +
                ", current_ts='" + current_ts + '\'' +
                ", pos='" + pos + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", nation_code='" + nation_code + '\'' +
                ", nation_name='" + nation_name + '\'' +
                '}';
    }

    public SchemaOfOgg(String table, String op_type, String op_ts, String current_ts, String pos, JSONObject before, JSONObject after, String nation_code, String nation_name) {
        this.table = table;
        this.op_type = op_type;
        this.op_ts = op_ts;
        this.current_ts = current_ts;
        this.pos = pos;
        this.before = before;
        this.after = after;
        this.nation_code = nation_code;
        this.nation_name = nation_name;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public String getOp_ts() {
        return op_ts;
    }

    public void setOp_ts(String op_ts) {
        this.op_ts = op_ts;
    }

    public String getCurrent_ts() {
        return current_ts;
    }

    public void setCurrent_ts(String current_ts) {
        this.current_ts = current_ts;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public JSONObject getBefore() {
        return before;
    }

    public void setBefore(JSONObject before) {
        this.before = before;
    }

    public JSONObject getAfter() {
        return after;
    }

    public void setAfter(JSONObject after) {
        this.after = after;
    }

    public String getNation_code() {
        return nation_code;
    }

    public void setNation_code(String nation_code) {
        this.nation_code = nation_code;
    }

    public String getNation_name() {
        return nation_name;
    }

    public void setNation_name(String nation_name) {
        this.nation_name = nation_name;
    }
}
