package com.webworkz.playground.azure.common;

import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {
	void accept(T t) throws E;

	static <T, E extends Exception> Consumer<T> unchecked(ThrowingConsumer<T, Exception> throwingConsumer) {
		return T -> {
			try {
				throwingConsumer.accept(T);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		};
	}
}
