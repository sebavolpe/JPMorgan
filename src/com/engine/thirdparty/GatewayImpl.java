package com.engine.thirdparty;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class GatewayImpl implements Gateway {
	
	private static final Logger logger = Logger.getLogger(GatewayImpl.class);
	
	private static final long TIMEOUT_SECONDS = 100;
	
	private static Set<String> processedGroups = new HashSet<>(); // potential out of memory!
	

	public static boolean wasProcessed(String group) {
		return processedGroups.contains(group); 
	}
	

	@Override
	public void send(Message msg) {
		long before = System.currentTimeMillis();
		logger.info("start processing message id ="+msg.getId());
		processedGroups.add(msg.getGroup());
		Thread t1 = new Thread(new ThirdParty(msg)); // for performance we could use a thread pool (newFixedThreadPool)
		t1.start();

		synchronized(msg) {
			try {
				msg.wait(TIMEOUT_SECONDS * 1000);
			} catch (InterruptedException e) {
				logger.error("interrupted:"+e.getMessage(), e);
			}
		}
		long delay = System.currentTimeMillis() - before;
		logger.info("end processing message id="+msg.getId()+" delay="+delay+" milliseconds");
	}

}
