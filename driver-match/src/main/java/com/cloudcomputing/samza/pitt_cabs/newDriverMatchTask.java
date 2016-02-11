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
public class newDriverMatchTask implements StreamTask, InitableTask,
		WindowableTask {
	/* Define per task state here. (kv stores etc) */
	private KeyValueStore<String, Map<String, Object>> drivers;
	private Integer [] lastFiveDiverRatio = new Integer[5];
    private int count =0;
    private int sumRequest = 0;
    private int driverRatio =0;
	
	private final String ENTERING_BLOCK = "ENTERING_BLOCK";
	private final String LEAVING_BLOCK = "LEAVING_BLOCK";
	private final String RIDE_COMPLETE = "RIDE_COMPLETE";
	private final String RIDE_REQUEST = "RIDE_REQUEST";
	private final String AVAILABLE = "AVAILABLE";
	private final String UNAVAILABLE = "UNAVAILABLE";
	
	/**
	 * Initialize the KeyValueStore
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		// Initialize stuff (maybe the kv stores?)
		drivers = (KeyValueStore<String, Map<String, Object>>) context.getStore("driver-list");
		count =0;
		sumRequest = 0;
		driverRatio =0;
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
		Integer latitude = (Integer) driverLocation.get("latitude");
		Integer longitude = (Integer) driverLocation.get("longitude");
		Integer driver = (Integer) driverLocation.get("driverId");
		
		Map<String, Object> driverInfo = new HashMap<String, Object>();
		driverInfo.put("latitude", latitude);
		driverInfo.put("longitude", longitude);
		driverInfo.put("blockId", block);
		driverInfo.put("driverId", driver);
		driverInfo.put("status", AVAILABLE);
		drivers.put(String.valueOf(driver), driverInfo);
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
			String driverId = findNearstDriver(latitude, longitude, riderBlock);
			
			if (driverId != null) {
				drivers.get(driverId).put("status", UNAVAILABLE);
				Double newPriceFactor = getPriceFactor(driverRatio);
				processOut(driverId, riderId, collector,newPriceFactor);
				return;
			}
			System.out.println("Do not find any suitable driver");
			return;
		} else {
			// if the event is not the ride_request, then update the KeyValueStroe separately
			if (type.equals(ENTERING_BLOCK)) {
				updateEnterBlock(obj);
			} else if (type.equals(LEAVING_BLOCK)) {
				updateEnterBlock(obj);
			} else if (type.equals(RIDE_COMPLETE)) {
				updateRideComplete(obj);
			} else {
				System.out
						.println("************** exception from processEvent function***********");
			}
		}
	}

	private Double getPriceFactor(int curdriverRatio) {
		sumRequest++;
		Double newPriceFactor = 1.0;
		if(sumRequest<5){
			lastFiveDiverRatio[count] = curdriverRatio;
			count++;
			count=count%5;
			sumRequest++;
			return 1.0;
		}else{
			lastFiveDiverRatio[count] = curdriverRatio;
			Double average = (lastFiveDiverRatio[0]+lastFiveDiverRatio[1]+lastFiveDiverRatio[2]+
					lastFiveDiverRatio[3]+lastFiveDiverRatio[4])/5.0;
			if(average>=1.8){
				newPriceFactor = 1.0;
			}else{
				newPriceFactor = 10*(1.8-average)+1;
			}
			count++;
			count=count%5;
			driverRatio =0;
			return newPriceFactor;
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
		driverInfo.put("blockId", blockId);
		driverInfo.put("driverId", driverId);
		driverInfo.put("status", status);
		drivers.put(String.valueOf(driverId), driverInfo);
	}

	/**
	 * Write out the output stream
	 * @param driverId 
	 * @param riderId
	 * @param collector - Message Collector
	 */
	private void processOut(String driverId, Integer riderId,MessageCollector collector,Double priceFactor) {
		Map<String,Object> obj = new HashMap<String, Object>();
		obj.put("riderId", riderId);
		obj.put("driverId", Integer.valueOf(driverId));
		obj.put("priceFactor",String.valueOf(priceFactor));
		collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), obj));
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
		KeyValueIterator<String, Map<String, Object>> iterator = drivers.all();
		double distance = Float.MAX_VALUE;
		
		try {
			while (iterator.hasNext()) {
				String driverId = iterator.next().getKey();
				Map<String, Object> driverInfo = drivers.get(driverId);
				if (driverInfo.get("status").equals(AVAILABLE)
						&& driverInfo.get("blockId").equals(riderBlock)) {
					driverRatio++;
					Integer latitude =  (Integer) driverInfo.get("latitude");
					Integer longitude =  (Integer) driverInfo.get("longitude");
					double curDistance = getDistance(latitude,longitude, riderLa, riderLo);
					if (curDistance < distance) {
						distance = curDistance;
						choose = driverId;
					}
				}
			}
		}finally {
		      iterator.close();
	    }
		return choose;
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
	private void updateEnterBlock(Map<String, Object> obj) {
		Integer driverId = (Integer) obj.get("driverId");
		String status  = (String) obj.get("status");
		Integer latitude = (Integer) obj.get("latitude");
		Integer longitude = (Integer) obj.get("longitude");
		Integer blockId = (Integer) obj.get("blockId");
		
		Map<String, Object> driverInfo = new HashMap<String, Object>();
		driverInfo.put("latitude", latitude);
		driverInfo.put("longitude", longitude);
		driverInfo.put("blockId", blockId);
		driverInfo.put("driverId", driverId);
		driverInfo.put("status", status);
		drivers.put(String.valueOf(driverId), driverInfo);
	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		// this function is called at regular intervals, not required for this
		// project
	}
}
