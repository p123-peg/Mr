package com.ll.mr;

import com.ll.mrbean.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class ClickStreamPageViews {

    static class PageViewsStreamThreeMapper extends Mapper<LongWritable, Text, Text, WebLogBean>{
        Text k = new Text();
        WebLogBean v = new WebLogBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if(fields.length < 9) {
                return;
            }

            v.set("true".equals(fields[0]),fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);

            if(v.isValid()){
                k.set(v.getRemote_addr());
                context.write(k, v);
            }
        }
    }

    static class PageViewsStreamThreeReducer extends Reducer<Text, WebLogBean, NullWritable, Text>{

        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) {
            List<WebLogBean> beans = new ArrayList<>();
            Text v = new Text();
            NullWritable k = NullWritable.get();

            try {
                for(WebLogBean bean: values){
                    WebLogBean webLogBean = new WebLogBean();
                    try {
                        BeanUtils.copyProperties(webLogBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    };
                    beans.add(webLogBean);
                }

                //???bean???????????????????????????
                Collections.sort(beans, new Comparator<WebLogBean>() {
                    @Override
                    public int compare(WebLogBean o1, WebLogBean o2) {
                        try {
                            Date date = toDate(o1.getRemote_time_local());
                            Date date2 = toDate(o2.getRemote_time_local());
                            if(null == date || null == date2){
                                return 0;
                            }
                            return date.compareTo(date2);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return 0;
                        }
                    }
                });

                /*
                 * ???????????????????????????bean??????????????????visit???????????????visit???????????????page???????????????step
                */
                int step = 1;
                String session = UUID.randomUUID().toString();

                for(int i=0;i<beans.size();i++){
                    WebLogBean bean = beans.get(i);

                    if (i==0){
                        v.set(session+","+bean.getRemote_addr() + "," + bean.getRemote_time_local() + "," + bean.getRequest_method_url() + "," + step + ","  + bean.getHttp_user_agent() + "," + bean.getReponse_body_bytes() + ","
                                + bean.getStatus());
                        context.write(k, v);
                        continue;
                    }
                    // ????????????1???????????????????????????
                    if(1 == beans.size()){
                        v.set(session+","+bean.getRemote_addr() + "," + bean.getRemote_time_local() + "," + bean.getRequest_method_url() + "," + 1 + ","  + bean.getHttp_user_agent() + "," + bean.getReponse_body_bytes() + ","
                                    + bean.getStatus());
                        context.write(k, v);
                        session = UUID.randomUUID().toString();
                        break;
                    }


                    // ?????????????????????
                    long time = timeDiff(toDate(bean.getRemote_time_local()), toDate(beans.get(i - 1).getRemote_time_local()));
                    
                    // ????????????-???????????????<180????????????????????????????????????????????????
                    if(time < 180 * 60 * 1000){
                        step++;
                        v.set(session+","+key.toString()+"," + beans.get(i).getRemote_time_local() + "," +
                                beans.get(i).getRequest_method_url() + "," + step + ","
								+ beans.get(i).getHttp_user_agent() + "," + beans.get(i).getReponse_body_bytes() + "," + beans.get(i).getStatus());
						context.write(k, v);

                    }else {
                        // ??????????????????????????????step??????
                        step = 1;
                        session = UUID.randomUUID().toString();
                        // ????????????-???????????????>30??????????????????????????????????????????????????????step???????????????????????????visit
                        v.set(session+","+key.toString()+","+ beans.get(i).getRemote_time_local() + "," +
                                beans.get(i).getRequest_method_url() + "," + step + ","
								+ beans.get(i).getHttp_user_agent() + "," + beans.get(i).getReponse_body_bytes() + "," + beans.get(i).getStatus());
						context.write(k, v);

                    }
                    


                }
            } catch (ParseException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        private long timeDiff(Date time1, Date time2) throws ParseException {
			return time1.getTime() - time2.getTime();
		}


        private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStreamPageViews.class);

		job.setMapperClass(PageViewsStreamThreeMapper.class);
		job.setReducerClass(PageViewsStreamThreeReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
