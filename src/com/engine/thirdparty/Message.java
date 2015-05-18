package com.engine.thirdparty;

public interface Message {
	
	public void completed();
	
//	public void processing();
	
	public String getGroup();
	
	public String getId();
	
	public void process();
}
