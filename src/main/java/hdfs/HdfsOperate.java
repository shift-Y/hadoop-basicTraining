package hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
/**
 * 文件操作类
 *
 */
public class HdfsOperate {
    @Test
	public void testMK()  throws Exception{
		Configuration conf=new Configuration();  //设置namenode地址
		conf.set("fs.defaultFS", "hdfs://192.168.221.128:9000");
		//得到hdfs文件系统创建文件夹
		FileSystem f=FileSystem.get(conf); 
//		f.copyToLocalFile(new Path("/user/zyy/datacount"), new Path("D:\\testHadoop"));
//		f.mkdirs(new Path("/user/zyy/testHadoop"));
		//读取hdfs文件到内存
		FSDataInputStream fs=f.open(new Path("/user/zyy/datacount"));
		/* 第一种方式 */
		int len=0;
		byte[] buf=new byte[1024];
		while((len = fs.read(buf)) != -1){
			String str=new String(buf,0,len);
			System.out.println(str+"\n");
		}
		fs.close();
		
		/*第二种 用hadoop api 拷贝字节的方式*/
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		System.out.println(bos.toString());
		//拷贝字节
		IOUtils.copyBytes(fs, bos, 1024);
		IOUtils.closeStream(bos);
		IOUtils.closeStream(fs);
		/* 第三种 文件读取*/
		//注册url流处理工厂    java中没有hdfs协议，所以需要先注册
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url=new URL("hdfs://192.168.221.128:9000/usr/zyy/...txt");
		URLConnection con=url.openConnection();
		InputStream in=con.getInputStream();
		int length=0;
		byte[] b=new byte[1024];
		while((length=in.read(b)) != 0){
			String str=new String(b,0,length);
			System.out.println(str);
		}
		in.close();
		
	}
	@Test
    public void testUpload()throws Exception{
    	Configuration conf=new Configuration();  //设置namenode地址
		conf.set("fs.defaultFS", "hdfs://192.168.221.128:9000");
		FileSystem f=FileSystem.get(conf);
		//得到一个输入流
		InputStream in=new FileInputStream("d:\\testHadoop\\a.txt");  
		//创建一个输出流
		OutputStream out=f.create(new Path("/user/zyy/testHadoop/a.txt"));
		//使用hdfs工具类上传数据
		try{
			IOUtils.copyBytes(in, out, 1024,false);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
	@Test
	public void testDel()throws Exception{
		Configuration conf=new Configuration();  //设置namenode地址
		conf.set("fs.defaultFS", "hdfs://192.168.221.128:9000");
		FileSystem f=FileSystem.get(conf);
		f.delete(new Path("/user/zyy/testHadoop"),true);
	}
	//java 读取hdfs文件
	@Test
	public void readFile(){
		try{
			//注册URL流工厂，解决java没有hdfs协议的问题
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			//使用URL访问hdfs文件
			URL url=new URL("hdfs://192.168.221.128:9000/aa.txt");
			URLConnection conn=url.openConnection();
			//通过流访问文件
			InputStream is=conn.getInputStream();
			int len=0;
			byte[] buf=new byte[1024];
			while((len = is.read(buf)) != -1){
				String str=new String(buf,0,len);
				System.out.println(str+"\n");
			}
			is.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
