package com.engine.strategy;

import com.engine.Task;

public class ArrivalOrderStrategy<T> implements PrioritisationStrategy<T> {

	@Override
	public int compare(Object a1, Object a2) {
		Task task1 = (Task) a1;
		Task task2 = (Task) a2;
		return task1.getMessage().getSequence().compareTo(task2.getMessage().getSequence());
	}


}
