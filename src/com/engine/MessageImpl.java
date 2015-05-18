package com.engine;

import org.apache.log4j.Logger;

import com.engine.thirdparty.Message;

public class MessageImpl implements Message {
	
	private static final Logger logger = Logger.getLogger(Message.class);
	
	private String id;
	
	private Long sequence;
	
	private String group;
	

	private boolean completed = false;
	
	private long delay;
	
	private long completionTime;
	
	private boolean terminationFlag;


	public boolean isTerminationFlag() {
		return terminationFlag;
	}


	public void setTerminationFlag(boolean terminationFlag) {
		this.terminationFlag = terminationFlag;
	}


	public long getCompletionTime() {
		return completionTime;
	}


	@Override
	public void completed() {
		completionTime = System.currentTimeMillis();
		this.setCompleted(true);
		synchronized(this) {
			this.notify();
		}

	}
	

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}


	public boolean isCompleted() {
		return completed;
	}


	public void setCompleted(boolean completed) {
		this.completed = completed;
	}


	public long getDelay() {
		return delay;
	}


	public void setDelay(long delay) {
		this.delay = delay;
	}


	public Long getSequence() {
		return sequence;
	}


	public void setSequence(Long sequence) {
		this.sequence = sequence;
	}


	@Override
	public void process() {
		sleep(this.getDelay()); // simulate logic processing time
		this.completed();
	}
	
	private void sleep(long m) {
		try {
			Thread.sleep(m);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

}
