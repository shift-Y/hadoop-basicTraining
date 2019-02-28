package myhadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * map类的作用解析文件生成新的key ,value输出给reducer类
 * @author administrator
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    //hadoop没有序列号，不能用long,string类型
	//输入hello tom hello kitty,输出hello 1 tom 1 hello 1 kitty 1
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		//文件内容大致为：hello kitty /n  hello tom 
		//接收数据
		String line=value.toString();  //一行内容
		//切分数据
		String[] words=line.split(" ");
		//循环每一行，每个按空格分割的单词出现一次 记一
		for(String s:words){
			context.write(new Text(s), new LongWritable(1));
		}
		
	}
}
