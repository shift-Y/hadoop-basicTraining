package serializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 统计文件中手机号的流量使用情况
 * @author administrator
 *
 */
public class DataCount {
  public static void main(String[] args) throws Exception{
	  Configuration conf=new Configuration();
	  Job job=new Job(conf);
	  job.setJarByClass(DataCount.class);
	  job.setMapperClass(DCMapper.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(DataBean.class);
	  FileInputFormat.setInputPaths(job,new Path(args[0]));
	  
	  job.setReducerClass(DCReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(DataBean.class);
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	  job.setPartitionerClass(ProviderPartitioner.class);
	  job.setNumReduceTasks(Integer.parseInt(args[2]));  //如果不设置reducer的数量默认是1,参数由命令执行时传入
	  job.waitForCompletion(true);                       //等待执行完成并打印结果
  }
  public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] str=line.split(" ");
		String telno=str[0];
		long up=Long.parseLong(str[1]);
		long down=Long.parseLong(str[2]);
		DataBean bean=new DataBean(telno,up,down);
		context.write(new Text(telno), bean);
	}
  }
 
  public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean>{

	@Override
	protected void reduce(Text key, Iterable<DataBean> v2s,
			Context context)throws IOException, InterruptedException {
		long up_sum=0;
		long down_sum=0;
		for(DataBean v2:v2s){
			up_sum+=v2.getUpPayLoad();
			down_sum+=v2.getDownPayLoad();
		}
		DataBean bean=new DataBean("",up_sum,down_sum);
		context.write(key, bean);
	}
  }
  //partitioner类在mapper执行完reducer执行前执行,实现将结果写入不同的分区中
  public static class ProviderPartitioner extends Partitioner<Text, DataBean>{
	private static Map<String,Integer> providerMap=new HashMap<String,Integer>();
	static{ //映射手机号前三位对应的分区编号 不正确举例：1 移动  2 联通 3 电信
		providerMap.put("135", 1);
		providerMap.put("136", 1);
		providerMap.put("137", 1);
		providerMap.put("150", 2);
		providerMap.put("186", 3);
	}
	 //方法返回分区号,分区的数量由reducer的个数决定
	@Override
	public int getPartition(Text key, DataBean value, int arg2) {
		String account=key.toString();
		String sub_acc=account.substring(0,3);
		Integer code=providerMap.get(sub_acc);
		if(code == null){
			return 0;
		}
		return code;
	}
  }
}
