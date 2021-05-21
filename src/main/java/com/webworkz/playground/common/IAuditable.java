package com.webworkz.playground.common;

import java.util.Map;

import com.webworkz.playground.azure.queue.ProcessingTypeState;

public interface IAuditable {
	long getStats(ProcessingTypeState processingTypeState);

	Map<ProcessingTypeState, Long> getStatsSnapshot();
}
