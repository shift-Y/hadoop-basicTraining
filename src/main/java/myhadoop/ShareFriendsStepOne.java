package myhadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * qq好友推荐 第一阶段
 *
 */
public class ShareFriendsStepOne {
	/**
	 * keyin 数据传入的k的类型
	 * valuein 数据传入的v的类型
	 * keyout  数据输出的k的类型
	 * valueout 数据输出的v的类型
	 */
   static class ShareFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//A:B C D （分别代表不同的qq号 ）
			//1 拿到数据
			String line=value.toString();
			//2数据切割
			String[] person_arr=line.split(":");
			//3拿到关键字段
			String person=person_arr[0];
			String friends=person_arr[1];
			//4输出到reduce阶段的格式
			for(String f:friends.split(" ")){
				context.write(new Text(f), new Text(person));
			}
		}
   }
   static class ShareFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{

	@Override
	protected void reduce(Text friend, Iterable<Text> persons,
			Context context)
			throws IOException, InterruptedException {
		//数据拼接
		StringBuffer bf=new StringBuffer();
		
		for(Text p:persons){
			bf.append(p).append(",");
		}
		//数据输出
		context.write(friend, new Text(bf.toString()));
	}
   }
}
