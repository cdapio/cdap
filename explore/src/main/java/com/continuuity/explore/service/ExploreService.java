package com.continuuity.explore.service;

import java.util.List;

/**
 * Interface for exploring datasets.
 */
public interface ExploreService {

  /**
   * Execute a Hive SQL statement asynchronously. Use the returned {@link Handle} to get the status/result of the
   * operation.
   * @param statement SQL statement.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException
   */
  Handle execute(String statement) throws ExploreException;

  /**
   * Fetch the status of a running Hive operation.
   * @param handle handle returned by {@link #execute(String)}.
   * @return status of the operation.
   * @throws ExploreException
   */
  Status getStatus(Handle handle) throws ExploreException;

  /**
   * Fetch the schema of the result of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.State#FINISHED}.
   * @param handle handle returned by {@link #execute(String)}.
   * @return list of {@link ColumnDesc} representing the schema of the results.
   * @throws ExploreException
   */
  List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException;

  /**
   * Fetch the results of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.State#FINISHED}. Can be called multiple times, until it returns an empty list
   *               indicating the end of results.
   * @param handle handle returned by {@link #execute(String)}.
   * @param size max rows to fetch in the call.
   * @return list of {@link Row}s.
   * @throws ExploreException
   */
  List<Row> nextResults(Handle handle, int size) throws ExploreException;

  /**
   * Cancel a running Hive operation. After the operation moves into a {@link Status.State#CANCELED},
   * {@link #close(Handle)} needs to be called to release resources.
   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException
   */
  void cancel(Handle handle) throws ExploreException;

  /**
   * Release resources associated with a Hive operation. After this call, handle of the operation becomes invalid.
   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException
   */
  void close(Handle handle) throws ExploreException;
}
