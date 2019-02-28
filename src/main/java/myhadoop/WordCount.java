package myhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**mapred代表的是hadoop旧API，而mapreduce代表的是hadoop新的API
 * 1.分析具体的业务逻辑，确定输入输出数据的样式
 * 2.自定义一个类，继承mapper类 重写map方法。实现具体的业务，将新的key value输出
 * 3.自定义一个类，继承reducer类 
 * 4.将自定义的mapper和reducer通过Job对象组装起来
 * @author administrator
 *
 */
public class WordCount {
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		//构建Job对象
		Job job = new Job(new Configuration());
		//main方法所在的类
		job.setJarByClass(WordCount.class);
		//设置mapper相关的属性
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));  //此路径是hadoop的hdfs文件系统上
		//设置reducer相关的属相
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交任务
		job.waitForCompletion(true);
	}
}
