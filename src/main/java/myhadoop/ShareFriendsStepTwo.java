package myhadoop;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * qq好友推荐 第二阶段
 *
 */
public class ShareFriendsStepTwo {
	static class ShareFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//A:B C D （分别代表不同的qq号 ）
			//1 拿到数据
			String line=value.toString();
			//2数据切割
			String[] friend_arr=line.split("\t");
			//3拿到关键字段
			String friend=friend_arr[0];
			String[] person=friend_arr[1].split(",");
			Arrays.sort(person);
			//4输出到reduce阶段的格式
			for(int i=0;i<person.length-2;i++){
				for(int j=0;j<person.length-1;j++){
					context.write(new Text(person[i]+"-"+person[j]), new Text(friend));
				}
			}
		}
   }
   static class ShareFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text>{

	@Override
	protected void reduce(Text persons, Iterable<Text> friends,
			Context context)
			throws IOException, InterruptedException {
		//数据拼接
		StringBuffer bf=new StringBuffer();
		
		for(Text f:friends){
			bf.append(f).append(" ");
		}
		//数据输出
		context.write(persons, new Text(bf.toString()));
		// 最后输入结果如：a-b C D E (c d e三个人都有两个共同好友a和b，所以c d e间可以互相推荐)
	}
   }
}
