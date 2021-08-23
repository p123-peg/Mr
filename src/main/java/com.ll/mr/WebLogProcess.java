package com.ll.mr;

import com.ll.mrbean.PageViewsBean;
import com.ll.mrbean.WebLogBean;
import com.ll.utils.WebLogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class WebLogProcess {
    //日志预处理mr程序

    static class WebLogPreProcessMapper extends Mapper<LongWritable, Text, LongWritable, WebLogBean>{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
        NullWritable v = NullWritable.get();
        WebLogBean k = new WebLogBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");
            //除去脏数据（比如一些一行中没有九列的数据）
            if(fields.length < 9) {
                return;
            }

            k.set("true".equals(fields[0]),fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);

            //以时间为key值，以行数据为value，作为map的输出
            LongWritable data = null;
            try {
                data = new LongWritable(sdf.parse(k.getRemote_time_local()).getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            context.write(data, k);


        }
    }
    static class WebLogPreProcessReduce extends Reducer<LongWritable, WebLogBean, WebLogBean, NullWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<WebLogBean> values , Context context)throws IOException,InterruptedException{

            for (WebLogBean value : values) {
                context.write(value, NullWritable.get());
            }


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogProcess.class);

        job.setMapperClass(WebLogPreProcessMapper.class);
        job.setReducerClass(WebLogPreProcessReduce.class);

        job.setMapOutputKeyClass(WebLogBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(WebLogBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);


    }
}
