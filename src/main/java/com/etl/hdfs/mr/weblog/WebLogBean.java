package com.etl.hdfs.mr.weblog;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// hadoop的数据类型

/**
 * 对接外部数据的层，表结构定义最好跟外部数据源保持一致
 * 术语： 贴源表
 */
public class WebLogBean implements Writable {

    // 日志信息
    private boolean valid = true; // 判断数据是否合法
    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String status;// 记录请求状态；成功是200
    private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
    private String http_referer;// 用来记录从那个页面链接访问过来的
    private String http_user_agent;// 记录客户浏览器的相关信息

    public void set(boolean valid,String remote_addr, String remote_user, String time_local, String request, String status, String body_bytes_sent, String http_referer, String http_user_agent) {
        this.valid = valid;
        this.remote_addr = remote_addr;
        this.remote_user = remote_user;
        this.time_local = time_local;
        this.request = request;
        this.status = status;
        this.body_bytes_sent = body_bytes_sent;
        this.http_referer = http_referer;
        this.http_user_agent = http_user_agent;
    }

    public boolean isValid() {
        return valid;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public String getTime_local() {
        return time_local;
    }

    public String getRequest() {
        return request;
    }

    public String getStatus() {
        return status;
    }

    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    /**
     * 重写toString方法
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        // \001 hive中的默认分隔符
        sb.append("\001").append(this.getRemote_addr());
        sb.append("\001").append(this.getRemote_user());
        sb.append("\001").append(this.getTime_local());
        sb.append("\001").append(this.getRequest());
        sb.append("\001").append(this.getStatus());
        sb.append("\001").append(this.getBody_bytes_sent());
        sb.append("\001").append(this.getHttp_referer());
        sb.append("\001").append(this.getHttp_user_agent());

        return sb.toString();
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(this.valid);
        dataOutput.writeUTF(null==remote_addr?"":remote_addr);
        dataOutput.writeUTF(null==remote_user?"":remote_user);
        dataOutput.writeUTF(null==time_local?"":time_local);
        dataOutput.writeUTF(null==request?"":request);
        dataOutput.writeUTF(null==status?"":status);
        dataOutput.writeUTF(null==body_bytes_sent?"":body_bytes_sent);
        dataOutput.writeUTF(null==http_referer?"":http_referer);
        dataOutput.writeUTF(null==http_user_agent?"":http_user_agent);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.valid = dataInput.readBoolean();
        this.remote_addr = dataInput.readUTF();
        this.remote_user = dataInput.readUTF();
        this.time_local = dataInput.readUTF();
        this.request = dataInput.readUTF();
        this.status = dataInput.readUTF();
        this.body_bytes_sent = dataInput.readUTF();
        this.http_referer = dataInput.readUTF();
        this.http_user_agent = dataInput.readUTF();
    }
}
