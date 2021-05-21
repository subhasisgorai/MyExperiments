package com.webworkz.playground.azure.common;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {
	R apply(T t) throws E;

	static <T, R, E extends Exception> Function<T, R> unchecked(ThrowingFunction<T, R, E> throwingFunction) {
		return T -> {
			try {
				return throwingFunction.apply(T);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		};
	}
}
