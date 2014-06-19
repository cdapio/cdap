package com.continuuity.explore.service;

import java.util.List;

/**
 * Interface for exploring datasets.
 */
public interface Explore {

  /**
   * Execute a Hive SQL statement asynchronously. The returned {@link Handle} can be used to get the status/result of
   * the operation.
   * @param statement SQL statement.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error executing statement.
   */
  Handle execute(String statement) throws ExploreException;

  /**
   * Fetch the status of a running Hive operation.
   * @param handle handle returned by {@link #execute(String)}.
   * @return status of the operation.
   * @throws ExploreException on any error fetching status.
   * @throws HandleNotFoundException when handle is not found.
   */
  Status getStatus(Handle handle) throws ExploreException, HandleNotFoundException;

  /**
   * Fetch the schema of the result of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.State#FINISHED}.
   * @param handle handle returned by {@link #execute(String)}.
   * @return list of {@link ColumnDesc} representing the schema of the results. Empty list if there are no results.
   * @throws ExploreException on any error fetching schema.
   * @throws HandleNotFoundException when handle is not found.
   */
  List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException, HandleNotFoundException;

  /**
   * Fetch the results of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.State#FINISHED}. Can be called multiple times, until it returns an empty list
   *               indicating the end of results.
   * @param handle handle returned by {@link #execute(String)}.
   * @param size max rows to fetch in the call.
   * @return list of {@link com.continuuity.explore.service.Row}s.
   * @throws ExploreException on any error fetching results.
   * @throws HandleNotFoundException when handle is not found.
   */
  List<Row> nextResults(Handle handle, int size) throws ExploreException, HandleNotFoundException;

  /**
   * Cancel a running Hive operation. After the operation moves into a {@link Status.State#CANCELED},
   * {@link #close(Handle)} needs to be called to release resources.
   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException on any error cancelling operation.
   * @throws HandleNotFoundException when handle is not found.
   */
  void cancel(Handle handle) throws ExploreException, HandleNotFoundException;

  /**
   * Release resources associated with a Hive operation. After this call, handle of the operation becomes invalid.
   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException on any error closing operation.
   * @throws HandleNotFoundException when handle is not found.
   */
  void close(Handle handle) throws ExploreException, HandleNotFoundException;
}
