package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
//排序类 (按收入多 和支出少进行排序)
public class InfoBean implements WritableComparable<InfoBean>{
    private String account;   //用户名
    private Double income;    //收入
    private Double expenses;  //支出
    private Double surplus;   //结余
    public void set(String account,Double income,Double expenses){
    	this.account=account;
    	this.income=income;
    	this.expenses=expenses;
    	this.surplus=income-expenses;
    }
    
	@Override
	public int compareTo(InfoBean o) {
		if(this.income==o.getIncome()){
			return this.expenses>o.getExpenses() ? 1:-1;
		}else{
			return this.income>o.getIncome()? -1:1;
		}
	}

	@Override
	public String toString() {
		return this.income+"\t"+this.expenses+"\t"+this.surplus;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.account=in.readUTF();
		this.income=in.readDouble();  //按顺序读即可读出对应的值
		this.expenses=in.readDouble();
		this.surplus=in.readDouble();
	}
    
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public Double getIncome() {
		return income;
	}

	public void setIncome(Double income) {
		this.income = income;
	}

	public Double getExpenses() {
		return expenses;
	}

	public void setExpenses(Double expenses) {
		this.expenses = expenses;
	}

	public Double getSurplus() {
		return surplus;
	}

	public void setSurplus(Double surplus) {
		this.surplus = surplus;
	}

}
