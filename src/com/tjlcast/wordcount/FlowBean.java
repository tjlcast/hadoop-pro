package com.tjlcast.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private String telephone ;
	private long upFlow ;
	private long downFlow ;
	private long totalFlow ;
	
	public FlowBean() {} ;
	
	public FlowBean(String telephone, long upFlow, long downFlow, long totalFlow) {
		super();
		this.telephone = telephone;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = totalFlow ;
	}
	
	public void setFields(String telephone, long upFlow, long downFlow, long totalFlow) {
		setTelephone(telephone);
		setUpFlow(upFlow);
		setDownFlow(downFlow);
		setTotalFlow(totalFlow);
	}

	public String getTelephone() {
		return telephone;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		telephone = in.readUTF() ;
		upFlow = in.readLong() ;
		downFlow = in.readLong() ;
		totalFlow = in.readLong() ;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telephone);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(totalFlow);
	}

	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public String toString() {
		return "telephone: "+telephone+" upFlow: "+upFlow+" downFlow: "+downFlow ;
	}

	@Override
	public int compareTo(FlowBean fb) {
		String fgTel = fb.getTelephone() ;
		return getTelephone().compareTo(fgTel);
	}
}
