package myhadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * reducer类的作用，按传过来的解析后的key value生成结果
 * @author administrator
 *
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	//经过shuffle（能处理多个mapper）处理后输入hello {1,1} tom {1,1} kitty {1} 输出结果大致为 ：hello 2 ,kitty 1
	@Override
	protected void reduce(Text key, Iterable<LongWritable> v2s,
			Context context)throws IOException, InterruptedException {
		//接收数据
		//定义一个计数器
		Long count=0L;
		//循环
		for(LongWritable v2: v2s){
			count+=v2.get();
		}
		//输出
		context.write(key,new LongWritable(count));
	}

}
