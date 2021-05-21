package com.webworkz.playground.pipeline;

public interface Startable {
	public abstract boolean isStarted();

	public abstract void start();

	public abstract void startCascaded();
}
