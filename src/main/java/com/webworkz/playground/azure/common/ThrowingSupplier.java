package com.webworkz.playground.azure.common;

import java.util.function.Supplier;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
	T get() throws E;

	static <T, E extends Exception> Supplier<T> unchecked(ThrowingSupplier<T, E> throwingSupplier) {
		return () -> {
			try {
				return throwingSupplier.get();
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		};
	}
}
