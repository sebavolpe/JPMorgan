package com.engine;

import org.apache.log4j.Logger;

import com.engine.thirdparty.GatewayImpl;

public class Task implements Runnable {

	private static final Logger logger = Logger.getLogger(Task.class);
	
	private MessageImpl msg;
	
	private Scheduler scheduler;
	
	public Task(MessageImpl msg, Scheduler scheduler) {
		this.msg = msg;
		this.scheduler = scheduler;
	}
	
	/* 
	 * Called when a message is pulled from the queue
	 */
	@Override
	public void run() {
		if (scheduler.wasCancelled(msg)) {
			logger.info("Ignoring cancelled message id ="+msg.getId());
		} else {
			GatewayImpl gateway = new GatewayImpl();
			gateway.send(msg);
		}
	}
	
	public MessageImpl getMessage() {
		return msg;
	}

	public void setMessage(MessageImpl msg) {
		this.msg = msg;
	}

}
