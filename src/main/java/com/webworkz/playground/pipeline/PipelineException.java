package com.webworkz.playground.pipeline;

public class PipelineException extends RuntimeException {
	private static final long serialVersionUID = 3459733290908373777L;

	public PipelineException() {
		super();
	}

	public PipelineException(String message) {
		super(message);
	}

	public PipelineException(String message, Throwable cause) {
		super(message, cause);
	}
}