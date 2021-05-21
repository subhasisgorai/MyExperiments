package com.webworkz.playground.azure.dataaccess;

import java.util.List;

import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;

/**
 * Reactive interface that defines various data access methods and operations
 * over a data storage service.
 *
 * @param <T> Parameterized type of data stored in the storage
 */
public interface IDataAccessObject<T> {

	/**
	 * This method tries to load all the entities in a given partition from the
	 * underlying storage system.
	 * 
	 * @param partitionKey identifies the partition from where entities need to be
	 *                     fetched
	 * @return {@link Flowable} of entities of type T for given partition
	 */
	Flowable<T> loadAllEntitiesInPartition(final String partitionKey);

	/**
	 * Retrieves entities in a partition of underlying storage systems. Executes
	 * retrieval query in segmented or paginated mode with
	 * {@link ResultContinuation} continuation token. <br>
	 * 
	 * @param partitionKey for the partition from where entities to be retrieved
	 * @return Flowable of entities of type <T>
	 */
	Flowable<T> loadAllEntitiesInPartitionSegmented(final String partitionKey);

	/**
	 * Retrieves entities given a partition range and a bunch of AND-filters. The
	 * query execution would be segmented/paged in this case.
	 * 
	 * @param startPartitionKey starting partition key of the range
	 * @param endPartitionKey   ending partition key of the range
	 * @param andFilters        collection of {@link TableQuery} filters to be
	 *                          combined with {@link Operators#AND}
	 * @return Flowable of entities of type <T>
	 */
	Flowable<T> loadAllEntitiesInPartitionsSegmented(final String startPartitionKey, final String endPartitionKey,
			final String... andFilters);

	/**
	 * Retrieves entities from underlying storage for the given collection of
	 * partition keys.
	 * 
	 * @param partitionKeys collection of partition keys for which the entities need
	 *                      to be retrieved
	 * @param andFilters    collection of additional filters, all the filters would
	 *                      be combined by AND operator
	 * @return Flowable of entities of type <T> for the given partition keys and
	 *         filters
	 */
	Flowable<T> loadAllEntitiesInPartitionsSegmented(final String[] partitionKeys, final String... andFilters);
	
	/**
	 * Retrieves entities from underlying storage for the given {@link Flowable} of
	 * partition keys.
	 * 
	 * @param partitionKeys      {@link Flowable} of partition keys for which the
	 *                           entities need to be retrieved
	 * @param partitionBatchSize during Entity fetching
	 *                           <code>partitionBatchSize</code> number of
	 *                           partitions keys are allowed in a single batch
	 * @param andFilters         collection of additional filters, all the filters
	 *                           would be combined by AND operator
	 * @return Flowable of entities of type <T> for the given partition keys and
	 *         filters
	 */
	Flowable<T> loadAllEntitiesInPartitionsSegmented(Flowable<String> partitionKeys, int partitionBatchSize,
			String... andFilters);
	
	/**
	 * This is just an overloaded version, only <code>partitionBatchSize</code> is
	 * not passed as a parameter rather a default value is taken as decided by the
	 * concrete implementations
	 * 
	 * @see IDataAccessObject#loadAllEntitiesInPartitionsSegmented(Flowable, int,
	 *      String...)
	 * 
	 * @param partitionKeys {@link Flowable} of partition keys for which the
	 *                      entities need to be retrieved
	 * @param andFilters    collection of additional filters, all the filters would
	 *                      be combined by AND operator
	 * @return Flowable of entities of type <T> for the given partition keys and
	 *         filters
	 */
	Flowable<T> loadAllEntitiesInPartitionsSegmented(Flowable<String> partitionKeys, String... andFilters);

	/**
	 * Persists a new entity in the underlying entity storage.
	 * 
	 * @param entity to be persisted
	 * @return {@link Completable} that notifies the subscriber in case of success
	 *         or failure once the insert operation is completed
	 */
	Completable insert(final T entity);

	/**
	 * Updates an entity in the underlying entity storage.
	 * 
	 * @param entity to be updated
	 * @return {@link Completable} that notifies the subscriber in case of success
	 *         or failure once the update operation is completed
	 */
	Completable update(final T entity);

	/**
	 * Updates an existing entity but in a batch mode.
	 * 
	 * @param entities  collection of entities to be updated
	 * @param batchSize size of a batch
	 * @return {@link Completable} that notifies the subscriber in case of success
	 *         or failure once the batch update operation is completed
	 */
	Completable batchMerge(List<T> entities, int batchSize);

	/**
	 * Deletes an entity from the underlying storage.
	 * 
	 * @param entity the entity being deleted
	 * @return {@link Completable} that notifies the subscriber in case of success
	 *         or failure once the deletion operation is completed
	 */
	Completable delete(final T entity);

	/**
	 * Retrieves the unique entity with the given partition key and row key from the
	 * underlying storage.
	 * 
	 * @param partitionKey partition key for the entity to be retrieved
	 * @param rowKey       row key for the entity to be retrieved
	 * @return A {@link Maybe} with the entity result for the given partition key
	 *         and row key, typically a MayBe represents a defer computation with an
	 *         eventual result of a single value or no value or even an Exception
	 */
	Maybe<T> get(final String partitionKey, final String rowKey);

}
