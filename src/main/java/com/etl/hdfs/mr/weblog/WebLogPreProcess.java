package com.etl.hdfs.mr.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 处理原始日志，过滤出真实pv请求 转换时间格式 对缺失字段填充默认值 对记录标记valid和invalid(是否符合后续处理要求)
 */
public class WebLogPreProcess {
    static class WebLogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // 存储网站url分类数据
        Set<String> pages = new HashSet<String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();

        /**
         * 从外部配置文件中加载网站的有用url分类数据 存储到maptask的内存中，用来对日志数据进行过滤
         * 静态资源过滤
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
        }

        /**
         * 静态资源过滤
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            if (webLogBean != null) {
                // 过滤js/图片/css等静态资源
                WebLogParser.filtStaticResource(webLogBean, pages);

                // 作为key
                k.set(webLogBean.toString());
                context.write(k, v);
            }
        }

        public static void main(String[] args) throws Exception {
/*            String inPath = args[0];
            String outPath = args[1];*/

            String inPath = "G:\\CODE\\data\\web_etl\\weblog\\input\\access.log.20181101.dat";
            String outPath = "G:\\CODE\\data\\web_etl\\weblog\\output";

            // 创建一个job和任务入口
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            // main方法所在的class
            job.setJarByClass(WebLogPreProcess.class);

            // 指定job的Mapper
            job.setMapperClass(WebLogPreProcessMapper.class);

            // 输出的数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 输入输出
            FileInputFormat.setInputPaths(job, new Path(inPath));
            FileOutputFormat.setOutputPath(job, new Path(outPath));

            // 设置reducetask个数为0，即没有reduce阶段
            job.setNumReduceTasks(0);

            boolean res = job.waitForCompletion(true);
            System.exit(res?0:1);

        }
    }
}
