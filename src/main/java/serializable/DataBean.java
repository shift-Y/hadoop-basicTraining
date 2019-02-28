package serializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * hadoop 自定义序列化类
 * @author administrator
 *
 */
public class DataBean implements Writable{
    private String telNo;
    private long upPayLoad;   // 上行流量
    private long downPayLoad; // 下行流量
    private long totalPayLoad;// 总流量
    public DataBean(){}  //如果类中没有构造方法时，编译器会自动添加一个无参构造方法，否则如果有人为添加的构造方法则必须再加一个无参构造方法便于创建对象
    //有参构造方法传入参数，否则调用get set 方法比较麻烦
    public DataBean(String telNo,long upPayLoad,long downPayLoad){
    	this.telNo=telNo;
    	this.upPayLoad=upPayLoad;
    	this.downPayLoad=downPayLoad;
    	this.totalPayLoad=upPayLoad+downPayLoad;
    }
    //反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.telNo=in.readUTF();
		this.upPayLoad=in.readLong();
		this.downPayLoad=in.readLong();
		this.totalPayLoad=in.readLong();
	}
    //序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telNo);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
		out.writeLong(totalPayLoad); //将内容写到字节流中
	}
    @Override
    public String toString(){
    	return this.upPayLoad+" "+this.downPayLoad+" "+this.totalPayLoad;
    }
	public String getTelNo() {
		return telNo;
	}

	public void setTelNo(String telNo) {
		this.telNo = telNo;
	}

	public Long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public Long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public Long getTotalPayLoad() {
		return totalPayLoad;
	}

	public void setTotalPayLoad(long totalPayLoad) {
		this.totalPayLoad = totalPayLoad;
	}
	

}
