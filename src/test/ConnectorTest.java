package test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.engine.MessageImpl;
import com.engine.Scheduler;
import com.engine.TerminatedMessageException;
import com.engine.strategy.ArrivalOrderStrategy;
import com.engine.strategy.GroupStrategy;

public class ConnectorTest {
	
	@Test
	public void testUsingGroupStrategyMessageGroupInterlaces() {
		MessageImpl msg1 = buildMessage("A1", "A", 1000);
		MessageImpl msg2 = buildMessage("A2", "A", 1000);
		MessageImpl msg3 = buildMessage("A3", "A", 1000);
		MessageImpl msg4 = buildMessage("B4", "B", 1000);
		MessageImpl msg5 = buildMessage("A5", "A", 1000); // arrive late but should be processed before B4
		
		List<MessageImpl> list = new ArrayList<>();
		list.add(msg1);
		list.add(msg2);
		list.add(msg3);
		list.add(msg4);
		list.add(msg5);
		
		Scheduler receiver = new Scheduler(2, GroupStrategy.class.getSimpleName());
		
		try {
			receiver.onMessage(msg1);
			receiver.onMessage(msg2);
			receiver.onMessage(msg3);
			receiver.onMessage(msg4);
			receiver.onMessage(msg5);
		} catch (TerminatedMessageException e) {
			Assert.fail();
		}
		
		while (true) {
			sleep(10);
			int completed = 0;
			for(MessageImpl msg : list) {
				if (msg.isCompleted()) {
					completed++;
				}
			}
			if (completed == list.size()) {
				break;
			}
		}
		
		long gap3to2 = msg3.getCompletionTime() - msg2.getCompletionTime();
		long gap3to1 = msg3.getCompletionTime() - msg1.getCompletionTime();
		long gap1to2 = Math.abs(msg1.getCompletionTime() - msg2.getCompletionTime());
		long gap5to3 = Math.abs(msg5.getCompletionTime() - msg3.getCompletionTime());
		long gap4to5 = msg4.getCompletionTime() - msg5.getCompletionTime();
		
		boolean ok3to2 = gap3to2 > 990;
		boolean ok3to1 = gap3to1 > 990;
		boolean ok1to2 = gap1to2 < 10;
		boolean ok5to3 = gap5to3 < 10;
		boolean ok4to5 = gap4to5 > 990;
		
		Assert.assertTrue(ok3to2);
		Assert.assertTrue(ok3to1);
		Assert.assertTrue(ok1to2);
		Assert.assertTrue(ok5to3);
		Assert.assertTrue(ok4to5);
	}
	
	@Test
	public void testArrivalOrderStrategy() {
		MessageImpl msg1 = buildMessage("A1", "A", 1000);
		MessageImpl msg2 = buildMessage("A2", "A", 1000);
		MessageImpl msg3 = buildMessage("A3", "A", 1000);
		
		List<MessageImpl> list = new ArrayList<>();
		list.add(msg1);
		list.add(msg2);
		list.add(msg3);
		
		Scheduler receiver = new Scheduler(2, ArrivalOrderStrategy.class.getSimpleName());
		
		try {
			receiver.onMessage(msg1);
			receiver.onMessage(msg2);
			receiver.onMessage(msg3);
		} catch (TerminatedMessageException e) {
			Assert.fail();
		}
		
		while (true) {
			sleep(10);
			int completed = 0;
			for(MessageImpl msg : list) {
				if (msg.isCompleted()) {
					completed++;
				}
			}
			if (completed == list.size()) {
				break;
			}
		}
		
		long gap3to2 = msg3.getCompletionTime() - msg2.getCompletionTime();
		long gap3to1 = msg3.getCompletionTime() - msg1.getCompletionTime();
		long gap1to2 = Math.abs(msg1.getCompletionTime() - msg2.getCompletionTime());
		
		boolean ok3to2 = gap3to2 > 990;
		boolean ok3to1 = gap3to1 > 990;
		boolean ok1to2 = gap1to2 < 10;
		Assert.assertTrue(ok3to2);
		Assert.assertTrue(ok3to1);
		Assert.assertTrue(ok1to2);
	}
	
	@Test
	public void testUsingGroupStrategyMessageGroupCanceled() {
		MessageImpl msg1 = buildMessage("A1", "A", 1000);
		MessageImpl msg2 = buildMessage("A2", "A", 1000);
		MessageImpl msg3 = buildMessage("A3", "A", 1000);
		MessageImpl msg4 = buildMessage("B4", "B", 1000);
		MessageImpl msg5 = buildMessage("A5", "A", 1000); // arrive late but should be processed before B4
		
		List<MessageImpl> list = new ArrayList<>();
		list.add(msg1);
		list.add(msg2);
		list.add(msg3);
		list.add(msg4);
		list.add(msg5);
		
		Scheduler receiver = new Scheduler(2, GroupStrategy.class.getSimpleName());
		
		try {
			receiver.onMessage(msg1);
			receiver.onMessage(msg2);
			receiver.onMessage(msg3);
			receiver.onMessage(msg4);
			receiver.onMessage(msg5);
		} catch (TerminatedMessageException e) {
			Assert.fail();
		}
		receiver.cancelGroup("B");
		
		sleep(7*1000); // wait until all the messages are processed
		
		long gap3to2 = msg3.getCompletionTime() - msg2.getCompletionTime();
		long gap3to1 = msg3.getCompletionTime() - msg1.getCompletionTime();
		long gap1to2 = Math.abs(msg1.getCompletionTime() - msg2.getCompletionTime());
		long gap5to3 = Math.abs(msg5.getCompletionTime() - msg3.getCompletionTime());
		
		boolean ok3to2 = gap3to2 > 990;
		boolean ok3to1 = gap3to1 > 990;
		boolean ok1to2 = gap1to2 < 10;
		boolean ok5to3 = gap5to3 < 10;
		boolean ok4 = !msg4.isCompleted();
		
		Assert.assertTrue(ok3to2);
		Assert.assertTrue(ok3to1);
		Assert.assertTrue(ok1to2);
		Assert.assertTrue(ok5to3);
		Assert.assertTrue(ok4);
	}
	
	@Test
	public void testUsingGroupStrategyMessageGroupTerminated() {
		MessageImpl msg1 = buildMessage("A1", "A", 1000);
		MessageImpl msg2 = buildMessage("A2", "A", 1000);
		MessageImpl msg3 = buildMessage("A3", "A", 1000);
		msg3.setTerminationFlag(true);
		MessageImpl msg4 = buildMessage("B4", "B", 1000);
		MessageImpl msg5 = buildMessage("A5", "A", 1000); // should not be processed because the group was already terminated
		
		List<MessageImpl> list = new ArrayList<>();
		list.add(msg1);
		list.add(msg2);
		list.add(msg3);
		list.add(msg4);
		list.add(msg5);
		
		Scheduler receiver = new Scheduler(2, GroupStrategy.class.getSimpleName());
		
		try {
			receiver.onMessage(msg1);
			receiver.onMessage(msg2);
			receiver.onMessage(msg3);
			receiver.onMessage(msg4);
		} catch (TerminatedMessageException e) {
			Assert.fail();
		}
		
		try {
			receiver.onMessage(msg5);
			Assert.fail();
		} catch (TerminatedMessageException e) {
			// expected behavior
		}
		
		sleep(7*1000); // wait until all the messages are processed
		
		long gap3to2 = msg3.getCompletionTime() - msg2.getCompletionTime();
		long gap3to1 = msg3.getCompletionTime() - msg1.getCompletionTime();
		long gap1to2 = Math.abs(msg1.getCompletionTime() - msg2.getCompletionTime());
		
		boolean ok3to2 = gap3to2 > 990;
		boolean ok3to1 = gap3to1 > 990;
		boolean ok1to2 = gap1to2 < 10;
		boolean ok4 = msg4.isCompleted();
		boolean ok5 = !msg5.isCompleted();
		
		Assert.assertTrue(ok3to2);
		Assert.assertTrue(ok3to1);
		Assert.assertTrue(ok1to2);
		Assert.assertTrue(ok4);
		Assert.assertTrue(ok5);
	}


	private MessageImpl buildMessage(String id, String group, long delay) {
		MessageImpl msg = new MessageImpl();
		msg.setId(id);
		msg.setGroup(group);
		msg.setDelay(delay);
		return msg;
	}

	private void sleep(long m) {
		try {
			Thread.sleep(m);
		} catch (InterruptedException e) {

		}
	}

}
