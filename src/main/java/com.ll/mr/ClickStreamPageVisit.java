package com.ll.mr;

import com.ll.mrbean.PageViewsBean;
import com.ll.mrbean.VisitBean;
import com.ll.mrbean.WebLogBean;
import com.ll.utils.WebLogParser;
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


public class ClickStreamPageVisit  {

    static class ClickStreamPageVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean>{
        PageViewsBean pageViewsBean = new PageViewsBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            String step = fields[4];
            pageViewsBean.set(fields[0], fields[1], fields[2], fields[3], step, fields[5], fields[6],fields[7]);
            k.set(pageViewsBean.getSession());
            context.write(k, pageViewsBean);
        }
    }

    static class ClickStreamPageVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, Text>{
        NullWritable k = NullWritable.get();
        Text v = new Text();
        VisitBean visitBean = new VisitBean();

        @Override
        protected void reduce(Text session, Iterable<PageViewsBean> pvBeans, Context context) throws IOException, InterruptedException {
            // 将pvBeans按照step排序
            ArrayList<PageViewsBean> beans = new ArrayList<>();
            for(PageViewsBean pvBean: pvBeans){
                PageViewsBean pvb = new PageViewsBean();
                try {
                    BeanUtils.copyProperties(pvb, pvBean);
                    beans.add(pvb);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

			Collections.sort(beans, new Comparator<PageViewsBean>() {
				@Override
				public int compare(PageViewsBean o1, PageViewsBean o2) {
                    try{
                        return Integer.parseInt(o1.getStep().trim())>Integer.parseInt(o2.getStep().trim())?1:-1;
                    }catch(Exception e){
                        return 0;
                    }

				}
			});
            Collections.sort(beans, new Comparator<PageViewsBean>() {
                @Override
                public int compare(PageViewsBean o1, PageViewsBean o2) {
                    try {
                        Date date = toDate(o1.getTimestr());
                        Date date2 = toDate(o2.getTimestr());
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
            // 取这次visit的首尾pageview记录，将数据放入VisitBean中
if(beans.size()==1){
    visitBean.setInPage(beans.get(0).getRequest_url());
    visitBean.setInTime(beans.get(0).getTimestr());
    visitBean.setOutPage(beans.get(0).getRequest_url());
    try {
        visitBean.setOutTime(WebLogParser.tuichi(beans.get(0).getTimestr()));
    } catch (ParseException e) {
        e.printStackTrace();
    }
    // visit访问的页面数
    visitBean.setPageVisits(beans.size());
    // 来访者的ip
    visitBean.setRemote_addr(beans.get(0).getRemote_addr());
    visitBean.setSession(session.toString());
    v.set(session.toString()+","+visitBean.getRemote_addr()+","+visitBean.getInTime()+","+visitBean.getInPage()+","+visitBean.getOutTime()+","+visitBean.getOutPage());
    context.write(k, v);
}else {
    // 取visit的首记录
    visitBean.setInPage(beans.get(0).getRequest_url());
    visitBean.setInTime(beans.get(0).getTimestr());

    // 取visit的尾记录
    visitBean.setOutPage(beans.get(beans.size() - 1).getRequest_url());
    visitBean.setOutTime(beans.get(beans.size() - 1).getTimestr());
    // 来访者的ip
    visitBean.setRemote_addr(beans.get(0).getRemote_addr());
    visitBean.setSession(session.toString());
    v.set(session.toString()+","+visitBean.getRemote_addr()+","+visitBean.getInTime()+","+visitBean.getInPage()+","+visitBean.getOutTime()+","+visitBean.getOutPage());
    context.write(k, v);
}



        }
        private Date toDate(String timeStr) throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
            return df.parse(timeStr);
        }
    }

    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStreamPageVisit.class);

		job.setMapperClass(ClickStreamPageVisitMapper.class);
		job.setReducerClass(ClickStreamPageVisitReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
