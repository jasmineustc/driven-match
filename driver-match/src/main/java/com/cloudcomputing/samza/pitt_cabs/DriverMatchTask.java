package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask,
		WindowableTask {
	/* Define per task state here. (kv stores etc) */
	private KeyValueStore<String, Map<String, Map<String,Object>>> drivers;
	private Map<Integer,myQueue> spf;
	
	private final String ENTERING_BLOCK = "ENTERING_BLOCK";
	private final String LEAVING_BLOCK = "LEAVING_BLOCK";
	private final String RIDE_COMPLETE = "RIDE_COMPLETE";
	private final String RIDE_REQUEST = "RIDE_REQUEST";
	private final String AVAILABLE = "AVAILABLE";
	
	/**
	 * Initialize the KeyValueStore
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		// Initialize stuff (maybe the kv stores?)
		drivers = (KeyValueStore<String, Map<String, Map<String,Object>>>) context.getStore("driver-list");
		spf = new HashMap<Integer,myQueue>();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void process(IncomingMessageEnvelope envelope,
			MessageCollector collector, TaskCoordinator coordinator)
			throws JSONException {
		// The main part of your code. Remember that all the messages for a
		// particular partition
		// come here (somewhat like MapReduce). So for task 1 messages for a
		// blockId will arrive
		// at one task only, thereby enabling you to do stateful stream
		// processing.

		String incomingStream = envelope.getSystemStreamPartition().getStream();
		Map<String, Object> obj = (Map<String, Object>) envelope.getMessage();
		if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
			processDriverLocation(obj,collector);
		} else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
			processOut("76",1234567, collector,9999);
			processEvent(obj, collector);
		} else {
			System.out.println("************** exception from process function***********");
			throw new IllegalStateException("Unexpected input stream: "
					+ envelope.getSystemStreamPartition());
		}
	}

	/**
	 * Process the Driver Location 
	 * @param driverLocation
	 * @param collector
	 */
	private void processDriverLocation(Map<String, Object> driverLocation,MessageCollector collector) {
	
		Integer block = (Integer) driverLocation.get("blockId");
		Integer driverId = (Integer) driverLocation.get("driverId");
		Map<String, Map<String,Object>> driversInBlock = drivers.get(String.valueOf(block));
		if(driversInBlock!=null){
			processOut(driverId.toString(),1234567, collector,333);
			
			Map<String,Object> driver = driversInBlock.get(String.valueOf(driverId));
			if(driver!=null && driver.get("status").equals(AVAILABLE)){
				Integer latitude = (Integer) driverLocation.get("latitude");
				Integer longitude = (Integer) driverLocation.get("longitude");
				String status = AVAILABLE;
				Map<String,Object> newDriver = new HashMap<String,Object>();
				newDriver.put("status", status);
				newDriver.put("latitude",latitude);
				newDriver.put("longitude", longitude);
				drivers.get(String.valueOf(block)).put(String.valueOf(driverId), newDriver);
			}
			
			processOut(driverId.toString(),1234567, collector,334);
		}
	}

	/**
	 * Process the event stream
	 * @param obj , input object of event stream
	 * @param collector message collector
	 */
	private void processEvent(Map<String, Object> obj,
			MessageCollector collector) {
		String type = (String) obj.get("type");
		if (type.equals(RIDE_REQUEST)) {
			Integer riderId = (Integer) obj.get("riderId");
			Integer riderBlock = (Integer) obj.get("blockId");
			Integer latitude =  (Integer) obj.get("latitude");
			Integer longitude = (Integer) obj.get("longitude");
			// if receive a rider request, then find the nearest driver
			processOut("127",1234567, collector,9999);
			String driverId = findNearstDriver(latitude, longitude, riderBlock);
			processOut("129",1234567, collector,9999);
			if (driverId != null) {
				String findDriver = driverId.split(":")[0];
				processOut(findDriver,123456789, collector,45678);
				drivers.get(String.valueOf(riderBlock)).remove(findDriver);
				int newPriceFactor = getPriceFactor(riderBlock,collector,driverId.split(":")[1]);
				processOut(findDriver, riderId, collector,newPriceFactor);
				return;
			}
			System.out.println("Do not find any suitable driver");
			return;
		} else {
			// if the event is not the ride_request, then update the KeyValueStroe separately
			if (type.equals(ENTERING_BLOCK)) {
				processOut("142",1234567, collector,9999);
				updateEnterBlock(obj,ENTERING_BLOCK);
			} else if (type.equals(LEAVING_BLOCK)) {
				processOut("145",1234567, collector,9999);
				updateLeaveBlock(obj,LEAVING_BLOCK);
			} else if (type.equals(RIDE_COMPLETE)) {
				processOut("148",1234567, collector,9999);
				updateRideComplete(obj);
			} else {
				System.out.println("************** exception from processEvent function***********");
			}
		}
	}

	/**
	 * update the event of leaving block
	 * @param obj
	 * @param lEAVING_BLOCK
	 */
	private void updateLeaveBlock(Map<String, Object> obj, String lEAVING_BLOCK) {
		Integer driverId = (Integer) obj.get("driverId");
		Integer blockId = (Integer) obj.get("blockId");
		drivers.get(String.valueOf(blockId)).remove(String.valueOf(driverId));	
	}

	private int getPriceFactor(Integer blockId,MessageCollector collector,String driverRatio) {
		myQueue q = spf.get(blockId);
		
		if(q.getSize()<4){
			processOut("1234567",7654321,collector,Integer.valueOf(driverRatio));
			return 1;
		}else{
			processOut("1234567",7654321,collector,19-2*(q.getAverage()+Integer.valueOf(driverRatio)));
			return 19-2*(q.getAverage()+Integer.valueOf(driverRatio));
		}
	}

	/**
	 * Update the Driver with Ride Complete event 
	 * @param obj, input stream of Driver Ride Complete event
	 */
	private void updateRideComplete(Map<String, Object> obj) {
		Integer driverId = (Integer) obj.get("driverId");
		String status  = AVAILABLE;
		Integer latitude = (Integer) obj.get("latitude");
		Integer longitude = (Integer) obj.get("longitude");
		Integer blockId = (Integer) obj.get("blockId");
		
		Map<String, Object> driverInfo = new HashMap<String, Object>();
		driverInfo.put("latitude", latitude);
		driverInfo.put("longitude", longitude);
		driverInfo.put("status", status);
		drivers.get(String.valueOf(blockId)).put(String.valueOf(driverId), driverInfo);
	}

	/**
	 * Write out the output stream
	 * @param driverId 
	 * @param riderId
	 * @param collector - Message Collector
	 */
	private void processOut(String driverId, Integer riderId,MessageCollector collector,int priceFactor) {
		Map<String,Object> obj = new HashMap<String, Object>();
		obj.put("riderId", riderId);
		obj.put("driverId", Integer.valueOf(driverId));
		obj.put("priceFactor",String.valueOf(priceFactor));
		collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), obj));
	}
	
	
	private void process(String driverId,MessageCollector collector){
		
	}

	/**
	 * Helper function, find the nearest driver in the rider block
	 * @param riderLa - rider latitude
	 * @param riderLo - rider longitude
	 * @param riderBlock - Integer, block number of rider
	 * @return String - driverId
	 */
	private String findNearstDriver(Integer riderLa, Integer riderLo,
			Integer riderBlock) {
		String choose = null;
		double distance = Float.MAX_VALUE;
		 Map<String, Map<String,Object>> driverInBlock = drivers.get(String.valueOf(riderBlock));
		 int driverRatio = 0;
		 for(String driverId: driverInBlock.keySet()){
			 if(driverInBlock.get(driverId).get("status").equals(AVAILABLE)){
				 Integer latitude = (Integer) driverInBlock.get(driverId).get("latitude");
				 Integer longitude = (Integer) driverInBlock.get(driverId).get("longitude");
				 driverRatio++;
				 double curDistance = getDistance(latitude,longitude, riderLa, riderLo);
				 if (curDistance < distance) {
						distance = curDistance;
						choose = driverId;
				 }
			 }
		 }
		
		 if(spf.get(riderBlock)==null){
			 myQueue q = new myQueue();
			 q.push(driverRatio);
			 spf.put(riderBlock, q);
		 }else{
			 spf.get(riderBlock).push(driverRatio);
		 }
		return choose+":"+driverRatio;
	}

	/**
	 * calculate the distance 
	 * @param firstLa - first latitude
	 * @param firstLo - first longitude
	 * @param secondLa - second latitude
	 * @param secondLo - second longitude
	 * @return double- return the Euclidean distance between two point
	 */
	private double getDistance(Integer firstLa, Integer firstLo, Integer secondLa,
			Integer secondLo) {
		double length = 0;
		length = Math.pow(Math.abs(firstLa- secondLa), 2)+ Math.pow(Math.abs(firstLo-secondLo), 2);
		return length;
	}

	/**
	 * Update the KeyValueStore of Driver Enter_Block event
	 * @param obj- input object of event stream.
	 */
	private void updateEnterBlock(Map<String, Object> obj,String type) {
		Integer driverId = (Integer) obj.get("driverId");
		String status  = (String) obj.get("status");
		Integer latitude = (Integer) obj.get("latitude");
		Integer longitude = (Integer) obj.get("longitude");
		Integer blockId = (Integer) obj.get("blockId");
		
		Map<String, Object> driverInfo = new HashMap<String, Object>();
		driverInfo.put("latitude", latitude);
		driverInfo.put("longitude", longitude);
		driverInfo.put("status", status);
		drivers.get(String.valueOf(blockId)).put(String.valueOf(driverId), driverInfo);
	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		// this function is called at regular intervals, not required for this
		// project
	}
	
	private class myQueue{
		private ArrayList<Integer> queue ;
		private int count;
		private final int num;
		
		public myQueue(){
			queue = new ArrayList<Integer>();
			count = 0;
			num = 4;
		}
		
		public void push(Integer driverRatio){
			if(queue.size()<4){
				queue.add(driverRatio);
				count ++;
				count = count%num;
			}else{
				queue.remove(count);
				queue.add(driverRatio);
				count ++;
				count = count%4;
			}
		}
		
		
		public int getSize(){
			return queue.size();
		}
		
		public int getAverage(){
			int average = 0;
			for(int i=0;i<num;i++){
				average = average + queue.get(i);
			}
			return average;
		}
	}
}
