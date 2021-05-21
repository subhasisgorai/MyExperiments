package com.webworkz.playground.pipeline;

public interface Stoppable {
	public abstract boolean isStopRequested();

	public abstract void requestStop();

	public abstract void requestStopCascaded();

	public abstract void stopPrematurely();

}
