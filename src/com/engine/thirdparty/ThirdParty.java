package com.engine.thirdparty;


public class ThirdParty implements Runnable {

	private Message msg = null;
	
	public ThirdParty(Message msg) {
		this.msg = msg;
	}

	@Override
	public void run() {
		msg.process();
	}
	

}
