package com.ll.utils;

import com.ll.mrbean.WebLogBean;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Set;


public class WebLogParser {

    private static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	private static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);


    public static WebLogBean parser(String line) throws ParseException {
	    //解析一行日志数据
        WebLogBean bean = new WebLogBean();
        String[] arr = line.split(" ");
        if(arr.length > 11){
            bean.setRemote_addr(arr[0]);
            bean.setRemote_user(arr[1]);
            String local_time = formatDate(arr[3].substring(1));
            if(null == local_time){
                bean.setValid(false);
            }else {
                bean.setRemote_time_local(local_time);
            }

            bean.setRequest_method_url(arr[6]);
			bean.setStatus(arr[8]);
			bean.setReponse_body_bytes(arr[9]);
			bean.setHttp_referer(arr[10]);

			//如果useragent元素较多，拼接useragent
            if(arr.length > 12){
                StringBuilder sb = new StringBuilder();
                for(int i=0;i<arr.length;i++){
                    sb.append(arr[i]);
                }
                bean.setHttp_user_agent(sb.toString());
            }else {
                bean.setHttp_user_agent(arr[11]);
            }

            if(Integer.parseInt(bean.getStatus()) > 400){
                bean.setValid(false);
            }

        }else {
            bean.setValid(false);
        }

	    return bean;
    }

    public static void filterStaticResource(WebLogBean webLogBean, Set<String> sets){
	    if(sets.contains(webLogBean.getRequest_method_url())){
	        webLogBean.setValid(false);
        }
    }

    private static String formatDate(String time_local) throws ParseException {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}

	}
   public static String tuichi(String tuichi) throws ParseException {

        Date date =df2.parse(tuichi);
        Calendar calendar  =   Calendar.getInstance();

        calendar.setTime(date); //需要将date数据转移到Calender对象中操作
        calendar.add(Calendar.HOUR,3);//把日期往后增加n天.正数往后推,负数往前移动
        date=calendar.getTime();
        return df2.format(date);
    }

}
