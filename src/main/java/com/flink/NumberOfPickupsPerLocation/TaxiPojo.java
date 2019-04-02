package com.myflink.taxipojo;

public class TaxiPojo {

	private String driverName ;
	private String pickUp;
	private long pickUpTimestamp;
	private String drop;
	private long dropTimestamp;
	private long finalTimestamp;
	
	public TaxiPojo(String driverName, String pickUp, long pickUpTimestamp, String drop, long dropTimestamp,
			long finalTimestamp) {
		super();
		this.driverName = driverName;
		this.pickUp = pickUp;
		this.pickUpTimestamp = pickUpTimestamp;
		this.drop = drop;
		this.dropTimestamp = dropTimestamp;
		this.finalTimestamp = finalTimestamp;
	}

	public String getDriverName() {
		return driverName;
	}

	public String getPickUp() {
		return pickUp;
	}

	public long getPickUpTimestamp() {
		return pickUpTimestamp;
	}

	public String getDrop() {
		return drop;
	}

	public long getDropTimestamp() {
		return dropTimestamp;
	}

	public long getFinalTimestamp() {
		return finalTimestamp;
	}
	@Override
	public String toString() {
		return driverName+","+pickUp+","+pickUpTimestamp+","+drop+","+dropTimestamp+","+finalTimestamp;
	}
	
	
	
}
