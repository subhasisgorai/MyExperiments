package com.webworkz.playground.azure.dataaccess.azure.tablestorage;

import static com.webworkz.playground.azure.common.Constants.PARENTHESES_END;
import static com.webworkz.playground.azure.common.Constants.PARENTHESES_START;
import static com.webworkz.playground.azure.common.Constants.PARTITION_KEY;
import static com.webworkz.playground.azure.common.Constants.WHITESPACE;
import static com.webworkz.playground.azure.common.ThrowingSupplier.unchecked;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

import java.lang.ref.SoftReference;
import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableEntity;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.azure.storage.table.TableRequestOptions;
import com.webworkz.playground.azure.common.ReplenishableFiniteSource;
import com.webworkz.playground.azure.dataaccess.IDataAccessObject;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.schedulers.Schedulers;

/**
 * A concrete implementation of {@link IDataAccessObject} built on top of
 * <a href=
 * "https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-overview">Azure
 * Cloud Table Storage Service</a>.
 *
 * @param <T> parameterized entity type
 * 
 * @see ReplenishableFiniteSource
 * @see LazyDAOCache
 */
@ThreadSafe
public abstract class AzureTableStorageAbstractDAO<T extends TableEntity> implements IDataAccessObject<T> {
	private static final int _100 = 100;
	private static final float DEFAULT_REPLENISH_THRESHOLD = 0.6f;
	private URI uri;
	private final TableRequestOptions tableRequestOptions;
	private final Class<T> inferredType;

	private final ThreadLocal<CloudTable> cloudTable = new ThreadLocal<CloudTable>() {
		@Override
		protected CloudTable initialValue() {
			try {
				return new CloudTable(uri);
			} catch (StorageException se) {
				throw new RuntimeException(
						format("Azure Storage CloudTable instance formation with url [%s] failed", uri.toString()), se);
			}
		}
	};

	private static final int MAX_OPERATIONS_ALLOWED_IN_A_BATCH = _100;
	private static final int MAX_PARTITIONS_ALLOWED_IN_A_BATCH = _100;
	private static final Logger logger = LoggerFactory.getLogger(AzureTableStorageAbstractDAO.class);

	@SuppressWarnings("unchecked")
	protected AzureTableStorageAbstractDAO(String sasURI, Optional<TableRequestOptions> tableRequestOptionsOptional)
			throws URISyntaxException, StorageException {
		Validate.notNull(sasURI);

		this.uri = new URI(sasURI);
		this.tableRequestOptions = tableRequestOptionsOptional.orElse(null);

		inferredType = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	protected AzureTableStorageAbstractDAO(String sasURI) throws URISyntaxException, StorageException {
		this(sasURI, Optional.ofNullable(null));
	}

	/**
	 * {@inheritDoc} <br>
	 * Typically this method relies upon
	 * <a href="http://msdn.microsoft.com/en-us/library/azure/dd179421.aspx">Query
	 * Entities</a> operation on the
	 * <a href="http://msdn.microsoft.com/en-us/library/azure/dd179423.aspx">Table
	 * Service REST API</a> to query the table. The underlying Azure storage library
	 * tries to lazy load the entities with a batch of 1000 entities at a time.<br>
	 * Returned entities could have a large memory footprint, hence in order to
	 * avoid out of memory situations, {@link SoftReference} is leveraged here. <br>
	 * If an object is the referent of a {@code SoftReference} the GC generally
	 * allows them to hang around for a longer period of time and may survive
	 * several cycles of garbage collection - as long as the JVM is able to reclaim
	 * 'enough' memory to survive.
	 * 
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartition(String partitionKey) {
		Validate.notBlank(partitionKey);

		String partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL,
				partitionKey);
		TableQuery<T> tableQuery = TableQuery.from(inferredType).where(partitionFilter);
		logger.debug("Executing Table Query with partition filter {}", partitionFilter);
		Iterable<T> result = cloudTable.get().execute(tableQuery, tableRequestOptions, null);
		SoftReference<Iterable<T>> softRef = freeUpStrongReference(result);

		return safelyGenerateFlowableFromIterable(softRef);
	}

	/**
	 * {@inheritDoc} <br>
	 * Query is executed with a {@link TableRequestOptions} object that specifies
	 * execution options such as retry policy and timeout settings for the
	 * operation, if provided during the instantiation of the DAO instance.
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartitionSegmented(String partitionKey) {
		Validate.notBlank(partitionKey);

		String partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL,
				partitionKey);

		return executeQuerySegmentedAndGetFlowableResult(partitionFilter);
	}

	/**
	 * {@inheritDoc} <br>
	 * Query is executed with a {@link TableRequestOptions} object that specifies
	 * execution options such as retry policy and timeout settings for the
	 * operation, if provided during the instantiation of the DAO instance.
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartitionsSegmented(String startPartitionKey, String endPartitionKey,
			String... andFilters) {
		Validate.notBlank(startPartitionKey, "Start Partition Key in the range query should not be null");
		Validate.notBlank(endPartitionKey, "End Partition Key in the range query should not be null");

		String startPartitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY,
				QueryComparisons.GREATER_THAN_OR_EQUAL, startPartitionKey);
		String endPartitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY,
				QueryComparisons.LESS_THAN_OR_EQUAL, endPartitionKey);
		String combinedFilter = TableQuery.combineFilters(startPartitionFilter, Operators.AND, endPartitionFilter);

		combinedFilter = combineAndFilters(combinedFilter, andFilters);

		return executeQuerySegmentedAndGetFlowableResult(combinedFilter);
	}

	/**
	 * {@inheritDoc} <br>
	 * Query is executed with a {@link TableRequestOptions} object that specifies
	 * execution options such as retry policy and timeout settings for the
	 * operation, if provided during the instantiation of the DAO instance. <br>
	 * The query execution and result fetching won't be blocking the calling thread
	 * and rather it would scheduled on Rx Scheduler for IO-bound work.
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartitionsSegmented(String[] partitionKeys, String... andFilters) {
		Validate.isTrue(isNotEmpty(partitionKeys));
		String queryFilter = getCombinedPartitionKeysFilter(partitionKeys);
		queryFilter = combineAndFilters(queryFilter, andFilters);

		return executeQuerySegmentedAndGetFlowableResult(queryFilter).subscribeOn(Schedulers.io());
	}

	/**
	 * {@inheritDoc} <br>
	 * 
	 * @see AzureTableStorageAbstractDAO#loadAllEntitiesInPartitionsSegmented(String[],
	 *      String...)
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartitionsSegmented(Flowable<String> partitionKeys, int partitionBatchSize,
			String... andFilters) {
		return partitionKeys.buffer(partitionBatchSize)
				.flatMap(partitionKeysBatched -> loadAllEntitiesInPartitionsSegmented(
						partitionKeysBatched.stream().toArray(String[]::new), andFilters));
	}

	/**
	 * {@inheritDoc} <br>
	 * 
	 */
	@Override
	public Flowable<T> loadAllEntitiesInPartitionsSegmented(Flowable<String> partitionKeys, String... andFilters) {
		return loadAllEntitiesInPartitionsSegmented(partitionKeys, MAX_PARTITIONS_ALLOWED_IN_A_BATCH, andFilters);
	}

	/**
	 * {@inheritDoc} <br>
	 * 
	 * @throws UnsupportedOperationException always as the method is yet not
	 *                                       implemented
	 */
	@Override
	public Completable insert(T entity) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc} <br>
	 * 
	 * @throws UnsupportedOperationException always as the method is yet not
	 *                                       implemented
	 */
	@Override
	public Completable update(T entity) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc} <br>
	 * This method performs multiple Entity updates in batches of a given batch
	 * size. <br>
	 * This method won't block the calling thread, the job execution would happen
	 * with the help of the Rx Scheduler meant for IO-bound task.
	 * 
	 * @see TableBatchOperation
	 */
	@Override
	// TODO Improvement needed, this implementation need to be reworked/verified
	public Completable batchMerge(List<T> entities, int batchSize) {
		Validate.isTrue(batchSize > 0 && batchSize <= MAX_OPERATIONS_ALLOWED_IN_A_BATCH);
		Validate.isTrue(CollectionUtils.isNotEmpty(entities));

		Map<String, List<T>> segregatedEntitiesByPartitionKey = segregateByPartitionKeyIfNeeded(entities);

		logger.debug("Executing Table Batch Operation for updating {} entities {}", entities.size(), entities);
		return Completable.fromRunnable(() -> {

			segregatedEntitiesByPartitionKey.forEach((partitionKey, segregatedEntities) -> {
				int count = 0;
				TableBatchOperation batchOperation = null;

				for (T entity : segregatedEntities) {
					if (batchOperation == null)
						batchOperation = new TableBatchOperation();
					batchOperation.merge(entity);
					logger.trace("Batch size: {}, number of entities: {}, count: {}", batchSize,
							segregatedEntities.size(), count);
					if (++count % batchSize == 0 || count == segregatedEntities.size()) {
						try {
							int currentBatchSize = batchOperation.size();
							logger.debug(
									"About to execute batch operation on cloud table with number of entities: {}, count: {}",
									currentBatchSize, count);
							cloudTable.get().execute(batchOperation, tableRequestOptions, null);
							logger.info("Successfully Executed the batch of {} entities", currentBatchSize);
						} catch (StorageException e) {
							throw new RuntimeException("Batch Merge of entities failed", e);
						}
						batchOperation.clear();
					}
				}
			});
		}).subscribeOn(Schedulers.io());

	}

	private Map<String, List<T>> segregateByPartitionKeyIfNeeded(List<T> entities) {
		return entities.parallelStream().collect(Collectors.groupingBy(T::getPartitionKey));
	}

	/**
	 * {@inheritDoc} <br>
	 * 
	 * @throws UnsupportedOperationException always as the method is yet not
	 *                                       implemented
	 */
	@Override
	public Completable delete(T entity) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc} <br>
	 * The deferred job of identifying and retrieving the correct entity is
	 * scheduled on Rx IO scheduler without blocking the calling thread.
	 */
	@Override
	public Maybe<T> get(String partitionKey, String rowKey) {
		return Maybe.fromCallable(() -> {
			Validate.notBlank(partitionKey);
			Validate.notBlank(rowKey);

			logger.debug("Trying to retrieve Entity with Partition Key [{}], Row Key [{}]", partitionKey, rowKey);
			TableOperation tableOperation = TableOperation.retrieve(partitionKey, rowKey, inferredType);

			return cloudTable.get().execute(tableOperation).<T>getResultAsType();

		}).subscribeOn(Schedulers.io());
	}

	public static <T> SoftReference<T> freeUpStrongReference(T object) {
		Validate.notNull(object);
		SoftReference<T> softRef = new SoftReference<>(object);
		object = null;
		return softRef;
	}

	public static <T> T safelyGetObjectFromSoftReference(SoftReference<T> softReference) {
		Validate.notNull(softReference);
		T referenced = softReference.get();
		if (referenced == null)
			throw new RuntimeException("Referenced object is taking too much of memory, JVM Heap memory low");
		return referenced;
	}

	/**
	 * This method tries generating a {@link Flowable} from a {@link SoftReference}
	 * to {@link Iterable}.
	 * 
	 * @param softRef {@code SoftReference} to the iterable
	 * @return Generated {@link Flowable}
	 */
	protected Flowable<T> safelyGenerateFlowableFromIterable(SoftReference<Iterable<T>> softRef) {
		Validate.notNull(softRef);

		Iterable<T> results = safelyGetObjectFromSoftReference(softRef);

		if (results != null)
			return Flowable.generate(results::iterator, (iterator, emitter) -> {
				if (iterator != null && iterator.hasNext())
					emitter.onNext(iterator.next());
				else
					emitter.onComplete();
			});
		else
			return Flowable.empty();

	}

	/**
	 * This method constructs a {@link TableQuery}, executes it and takes care of
	 * pagination. With the paginated results it constructs a {@link Supplier} and
	 * passes it to the {@link ReplenishableFiniteSource}. Finally generates a
	 * {@link Flowable} out of the {@link ReplenishableFiniteSource} instance.
	 * 
	 * @param filter Filter for the {@link TableQuery}
	 * @return A {@link Flowable} that is backed by the results of a TableQuery
	 */
	protected synchronized Flowable<T> executeQuerySegmentedAndGetFlowableResult(String filter) {
		Validate.notBlank(filter);

		logger.info("Executing Table Query with inferredType {}, filter {}", inferredType, filter);

		TableQuery<T> tableQuery = TableQuery.from(inferredType).where(filter);
		ResultSegmentHolder<T> resultSegmentHolder = new ResultSegmentHolder<>();
		Supplier<ResultSegment<T>> azureStorageTableSupplier = unchecked(() -> {
			ResultSegment<T> currentResultSegment = cloudTable.get()
					.executeSegmented(tableQuery,
							resultSegmentHolder.resultSegment == null ? null
									: resultSegmentHolder.resultSegment.getContinuationToken(),
							tableRequestOptions, null);
			resultSegmentHolder.resultSegment = currentResultSegment;
			return currentResultSegment;
		});

		return Flowable.generate(
				() -> new ReplenishableFiniteSource<>(azureStorageTableSupplier, DEFAULT_REPLENISH_THRESHOLD),
				(supplier, emitter) -> {
					T t = supplier.get();
					if (t != null)
						emitter.onNext(t);
					else
						emitter.onComplete();
				});
	}

	private String getCombinedPartitionKeysFilter(String[] partitionKeys) {
		Validate.isTrue(isNotEmpty(partitionKeys));

		String firstPartitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL,
				partitionKeys[0]);

		if (partitionKeys.length == 1)
			return firstPartitionFilter;
		else {
			StringBuilder filterBuilder = new StringBuilder();
			filterBuilder.append(PARENTHESES_START);
			filterBuilder.append(firstPartitionFilter);
			filterBuilder.append(PARENTHESES_END);
			for (int i = 1; i < partitionKeys.length; i++)
				filterBuilder.append(WHITESPACE).append(Operators.OR).append(WHITESPACE).append(PARENTHESES_START)
						.append(TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL,
								partitionKeys[i]))
						.append(PARENTHESES_END);

			return filterBuilder.toString();
		}
	}

	private String combineAndFilters(String existingFilter, String... andFilters) {
		if (isNotEmpty(andFilters)) {
			for (String filter : andFilters) {
				existingFilter = TableQuery.combineFilters(existingFilter, Operators.AND, filter);
			}
		}
		return existingFilter;
	}

	private static class ResultSegmentHolder<T> {
		public ResultSegment<T> resultSegment;
	}

}
