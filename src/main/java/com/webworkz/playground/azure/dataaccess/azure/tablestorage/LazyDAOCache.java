package com.webworkz.playground.azure.dataaccess.azure.tablestorage;

import static com.webworkz.playground.azure.common.ThrowingFunction.unchecked;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.table.TableEntity;
import com.webworkz.playground.azure.common.ThrowingFunction;

/**
 * {@link LazyDAOCache} lazily constructs and caches DAO for a given
 * {@code sasURI}. <br>
 * The DAO instance should be thread safe and hence can safely be shared across
 * threads, this will help by avoiding creation of multiple DAO instances for a
 * particular {@code sasURI}.
 *
 */
@ThreadSafe
public class LazyDAOCache<E extends AzureTableStorageAbstractDAO<? extends TableEntity>> {
	private final Map<String, E> entityDAORegistry;

	private static final Logger logger = LoggerFactory.getLogger(LazyDAOCache.class);

	public LazyDAOCache() {
		entityDAORegistry = new ConcurrentHashMap<>();
	}

	private <T> T computeAndCache(String key, Map<String, T> registry, Function<String, T> valueComputeFunction) {
		T computedResult = valueComputeFunction.apply(key);
		T oldValue = registry.putIfAbsent(key, computedResult);
		return oldValue == null ? computedResult : oldValue;
	}

	/**
	 * Gets a DAO for the given {@code sasURI} if it's readily available otherwise
	 * constructs it before returning to the client.
	 * 
	 * @param sasURI SAS URI for which the DAO is needed
	 * @return DAO instance for interacting with the SAS URI provided
	 */
	public E getOrCreateDAO(String sasURI, ThrowingFunction<String, E, Exception> daoInstantiator) {
		logger.trace("Getting DAO for sasURI [{}]", sasURI);
		final E result = entityDAORegistry.get(sasURI);
		return result == null ? computeAndCache(sasURI, entityDAORegistry, unchecked(daoInstantiator)) : result;
	}

}