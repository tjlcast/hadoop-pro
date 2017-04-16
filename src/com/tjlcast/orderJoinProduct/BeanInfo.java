package com.tjlcast.orderJoinProduct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class BeanInfo implements Writable{
	String id ;
	int flag ;
	
	Date date ;
	String pid ;
	int amount ;
	
	String name ;
	String category_id ;
	float price ;
	
	
	public BeanInfo(String id, Date date, String pid, int amount, String name, String category_id, float price,
			int flag) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
		this.flag = flag;
	}
	
	
	public BeanInfo() {
		this.id = "";
		this.date = new Date();
		this.pid = "";
		this.amount = 0;
		this.name = "";
		this.category_id = "";
		this.price = 0;
		this.flag = 0;
	}


	public void setAll(String id, Date date, String pid, int amount, String name, String category_id, float price,
			int flag) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
		this.flag = flag;
	}


	public String getId() {
		return id;
	}


	public Date getDate() {
		return date;
	}


	public String getPid() {
		return pid;
	}


	public int getAmount() {
		return amount;
	}


	public String getName() {
		return name;
	}


	public String getCategory_id() {
		return category_id;
	}


	public float getPrice() {
		return price;
	}


	public int getFlag() {
		return flag;
	}


	public void setId(String id) {
		this.id = id;
	}


	public void setDate(Date date) {
		this.date = date;
	}


	public void setPid(String pid) {
		this.pid = pid;
	}


	public void setAmount(int amount) {
		this.amount = amount;
	}


	public void setName(String name) {
		this.name = name;
	}


	public void setCategory_id(String category_id) {
		this.category_id = category_id;
	}


	public void setPrice(float price) {
		this.price = price;
	}


	public void setFlag(int flag) {
		this.flag = flag;
	}
	
	
	public void setOrderInfo(String id, Date date, String pid, int amount) {
		this.flag = 0 ;
		this.id = id ;
		this.date = date ;
		this.pid = pid ;
		this.amount = amount ;
	}
	
	
	public void setProductInfo(String id, String name, String category_id, float price) {
		this.flag = 1 ;
		this.id = id ;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
	}
	
	boolean isOrder () {
		return getFlag()==0 ;
	}

	@Override
	public String toString() {
		if (getFlag() == 0 ) {
			return "OrderInfo [id=" + id + ", date=" + date + ", pid=" + pid + ", amount=" + amount + "]";
		} 
		return "ProductInfo [id=" + id + ", name=" + name + ", category_id=" + category_id + ", price=" + price + "]";
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.date = new Date(in.readUTF());
		this.pid = in.readUTF();
		this.amount = in.readInt();
		this.name = in.readUTF();
		this.category_id = in.readUTF();
		this.price = in.readFloat();
		this.flag = in.readInt();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(date.toString());
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(name);
		out.writeUTF(category_id);
		out.writeFloat(price);
		out.writeInt(flag);
	}
	
}
