package com.engine.strategy;

import com.engine.MessageImpl;
import com.engine.Task;
import com.engine.thirdparty.GatewayImpl;

public class GroupStrategy<T> implements PrioritisationStrategy<T> {

	@Override
	public int compare(Object a1, Object a2) {
		Task task1 = (Task) a1;
		Task task2 = (Task) a2;
		MessageImpl msg1 = task1.getMessage();
		MessageImpl msg2 = task2.getMessage();
		String groupMsg1 = msg1.getGroup();
		String groupMsg2 = msg2.getGroup();
		boolean wasProcessedGroupMsg1 = GatewayImpl.wasProcessed(groupMsg1);
		boolean wasProcessedGroupMsg2 = GatewayImpl.wasProcessed(groupMsg2);
		
		if ( (wasProcessedGroupMsg1 && wasProcessedGroupMsg2) || (!wasProcessedGroupMsg1 && !wasProcessedGroupMsg2) ){
			return msg1.getSequence().compareTo(msg2.getSequence());
		} 
		
		if (wasProcessedGroupMsg1 && !wasProcessedGroupMsg2) { // !wasProcessedGroupMsg2 added for code documentation.
			return -1;
		} else if (wasProcessedGroupMsg2 && !wasProcessedGroupMsg1) {
			return 1;
		}
		throw new IllegalStateException("this should be unreachable code");
	}

}
