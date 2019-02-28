package inverse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InverseIndex {
 
	public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text k=new Text();
        private Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split(" ");
			FileSplit split=(FileSplit)context.getInputSplit() ;//得到输入切片
			String path=split.getPath().toString(); //得到文件路径 
			for(String s:words){
				k.set(s+"->"+path);
				v.set("1");
				context.write(k, v);
			}
		}
	}
	public static class IndexCombiner extends Reducer<Text, Text, Text, Text>{
		private Text k=new Text();
		private Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Context context)
				throws IOException, InterruptedException {
			String[] wordAndPath=key.toString().split("->");
			String word=wordAndPath[0];
			String path=wordAndPath[1];
			int counter=0;
			for(Text t:value){
				counter+=Integer.parseInt(t.toString());
			}
			k.set(word);
			v.set(path+"->"+counter);
			context.write(k, v);
		}
	}
	//拼接数据
	public static class IndexReducer extends Reducer<Text, Text, Text, Text>{
		private Text v=new Text();//防止每次执行时都要new一个text ,所以放在最外面
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Context context)
				throws IOException, InterruptedException {
			String result="";
			for(Text t:value){
				result+=t.toString()+"\t";
			}
			v.set(result);
			context.write(key,v); 
		}
		
	}
	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		job.setJarByClass(InverseIndex.class);
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(IndexCombiner.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));  //此路径是hadoop的hdfs文件系统上
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交任务
		job.waitForCompletion(true);
	}

}
