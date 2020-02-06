package com.etl.hdfs.mr.weblog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

public class WebLogParser {
    // 时间处理
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    /**
     * 处理数据
     * @param line
     * @return
     */
    public static WebLogBean parser(String line) {
        WebLogBean webLogBean = new WebLogBean();

        // 按照行内容分隔符进行数据的分割
        String[] arr = line.split(" ");

        // 过滤
        if (arr.length > 11) {
            webLogBean.setRemote_addr(arr[0]);
            webLogBean.setRemote_user(arr[1]);
            String time_local = formatDate(arr[3].substring(1));
            // 对时间格式进行分割
            if(null==time_local || "".equals(time_local)) time_local="-invalid_time-";
            webLogBean.setTime_local(time_local);
            webLogBean.setRequest(arr[6]);
            webLogBean.setStatus(arr[8]);
            webLogBean.setBody_bytes_sent(arr[9]);
            webLogBean.setHttp_referer(arr[10]);

            // 注意如果useragent元素较多，则需要拼接useragent
            if (arr.length > 12) {
                StringBuilder sb = new StringBuilder();
                // 从useragent开头处拼接
                for (int i = 11; i < arr.length; i ++) {
                    sb.append(arr[i]);
                }
                webLogBean.setHttp_user_agent(sb.toString());
            } else {
                webLogBean.setHttp_user_agent(arr[11]);
            }

            // 大于400则为http错误
            if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
                // 设置为非法数据
                webLogBean.setValid(false);
            }

            // 时间
            if("-invalid_time-".equals(webLogBean.getTime_local())){
                webLogBean.setValid(false);
            }
        } else {
            webLogBean = null;
        }

        return webLogBean;
    }

    public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
        // 如果没有包含指定的路径则是非法的数据(静态资源过滤等)
        if (!pages.contains(bean.getRequest())) {
            bean.setValid(false);
        }
    }

    /**
     * 格式化时间
     * @param time_local
     * @return
     */
    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (ParseException e) {
            return null;
        }
    }
}
