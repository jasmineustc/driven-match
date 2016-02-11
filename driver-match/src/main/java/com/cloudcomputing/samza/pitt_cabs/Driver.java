package com.cloudcomputing.samza.pitt_cabs;

public class Driver {
	
	private String status;
	private String driverId;
	private String blockId;
	private Location location;
	
	/**
	 * Constructor for driver-locations stream
	 * @param driverId
	 * @param blockId
	 * @param la
	 * @param lo
	 */
	public Driver(String driverId,String blockId,String la,String lo){
		this.driverId = driverId;
		location = new Location(la,lo);
		status = null;
		this.blockId = blockId;
	}
	
	
	/**
	 * Constructor for events
	 * @param driverId
	 * @param blockId
	 * @param status
	 * @param la
	 * @param lo
	 */
	public Driver(String driverId,String blockId,String status,String la,String lo){
		this.driverId = driverId;
		location = new Location(la,lo);
		this.blockId = blockId;
		this.status = status;
	}
	
	public String getStatus(){
		return status;
	}
	
	public String getDriverId(){
		return driverId;
	}
	
	public String getBlockId(){
		return blockId;
	}
	
	public void setBlockId(String newId){
		blockId = newId;
	}
	
	public void setStatus(String newStatus){
		status = newStatus;
	}
	
	public void updateLocation(String la,String lo){
		location = new Location(la,lo);
	}
	
	public double getDistance(String la,String lo){
		Location rider = new Location(la,lo);
		return location.getDistance(rider);
	}
}
