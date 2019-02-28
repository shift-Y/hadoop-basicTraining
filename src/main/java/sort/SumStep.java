package sort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
//交易信息求和类（数据格式如：用户名   收入  支出  ）
public class SumStep {
   public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
   private InfoBean bean=new InfoBean();
   private Text k=new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] fields=line.split("\t");
		String account=fields[0];
		Double in=Double.parseDouble(fields[1]);
		Double out=Double.parseDouble(fields[2]);
		k.set(account);
		bean.set(account, in, out);
		context.write(k, bean);
	}
   }
   public static class SumReducer extends Reducer<Text,InfoBean,Text,InfoBean>{
	   private InfoBean bean=new InfoBean();
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,
				Context context)
				throws IOException, InterruptedException {
			double in_sum=0;
			double out_sum=0;
			for(InfoBean value: values){
				in_sum+=value.getIncome();
				out_sum+=value.getExpenses();
			}
			bean.set("", in_sum, out_sum);
			context.write(key,bean);
		}
   }
   public static void main(String[] args) throws Exception{
		   Configuration conf=new Configuration();
		   Job job=new Job(conf);
		   job.setJarByClass(SumStep.class);
		  job.setMapperClass(SumMapper.class);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(InfoBean.class);
		  FileInputFormat.setInputPaths(job,new Path(args[0]));
		  
		  job.setReducerClass(SumReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(InfoBean.class);
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		  job.waitForCompletion(true); 
   }
}
