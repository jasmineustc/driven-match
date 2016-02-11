package com.cloudcomputing.samza.pitt_cabs;


public class Location {
	Integer latitude;
	Integer longitude;
	
	public Location(String la, String lo){
		latitude = Integer.valueOf(la);
		longitude = Integer.valueOf(lo);
	}
	
	public int getLa(){
		return latitude;
	}
	
	public int getLo(){
		return longitude;
	}
	
	public double getDistance(Location other){
		double length = 0;
		length =Math.pow(Math.abs(other.latitude-latitude),2)+ 
				Math.pow(Math.abs(other.longitude-longitude),2);
		return length;
	}
	
	@Override
	public boolean equals(Object other){
		if(other instanceof Location){
			Location o = (Location) other;
			if(o.latitude == latitude && o.longitude == longitude){
				return true;
			}else{
				return false;
			}
		}else {
			return false;
		}
	}
}
