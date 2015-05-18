package com.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.engine.strategy.ArrivalOrderStrategy;
import com.engine.strategy.GroupStrategy;
import com.engine.strategy.PrioritisationStrategy;
import com.engine.thirdparty.Message;

public class Scheduler {
	

	private static final Logger logger = Logger.getLogger(Scheduler.class);
	
	private static final AtomicLong seqGenerator = new AtomicLong(0);
	
	private Set<String> canceledGroups = new HashSet<>();
	
	private Set<String> terminatedGroups = new HashSet<>();
	
	int keepAliveTime =  0;

	private static final int INITIAL_CAPACITY = 1000;
	
	private ThreadPoolExecutor customQueuePool = null;
	
	static Map<String, PrioritisationStrategy<Runnable>> strategies = new HashMap<>();
	
	static {
		strategies.put(ArrivalOrderStrategy.class.getSimpleName(), new ArrivalOrderStrategy<Runnable>());
		strategies.put(GroupStrategy.class.getSimpleName(), new GroupStrategy<Runnable>());
	}

	
	public Scheduler(int resources, String strategyId) {
		
		PrioritisationStrategy<Runnable> strategy = strategies.get(strategyId);
		if (strategy == null) {
			throw new IllegalArgumentException("Missing strategy:"+strategyId);
		}
		
		BlockingQueue<Runnable> queue = new PriorityBlockingQueue<Runnable>(INITIAL_CAPACITY, strategy);
		
		customQueuePool = new ThreadPoolExecutor(
				resources, resources,
		        keepAliveTime, TimeUnit.SECONDS,
		        queue,
		        new RejectedExecutionHandler() {
		            @Override
		            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		                try {
		                    executor.getQueue().put(r);
		                } catch (InterruptedException e) {
		                    throw new RuntimeException("Interrupted while submitting task", e);
		                }
		            }
		        });
	}


	
	public void onMessage(MessageImpl msg) throws TerminatedMessageException {
		msg.setSequence(seqGenerator.incrementAndGet()); // maintain message arrival order
		String groupId = msg.getGroup();
		logger.info("onMessage receive message id ="+msg.getId()+" group ="+groupId+ " seq="+msg.getSequence());
		
		if (this.terminatedGroups.contains(groupId)) {
			throw new TerminatedMessageException("Group was already terminated, group id ="+groupId);
		}
		
		if (!wasCancelled(msg)) { // only as performance optimization, actual validation is made at dequeue time
			this.putInQueue(msg);
		}
		
		if (msg.isTerminationFlag()) {
			this.terminatedGroups.add(groupId);
		}
	}
	
	// extra credit feature cancellation
	public void cancelGroup(String groupId) {
		this.canceledGroups.add(groupId);
	}
	
	public boolean wasCancelled(Message msg) {
		return this.canceledGroups.contains(msg.getGroup());
	}
	
	// extra credit feature termination
	public void terminateGroup(String groupId) {
		this.terminatedGroups.add(groupId);
	}
	
	
	
	private void putInQueue(MessageImpl msg) {
		Task task = new Task(msg, this);
		customQueuePool.execute(task);
	}
}
