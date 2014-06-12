package com.continuuity.explore.service;

import com.google.common.util.concurrent.Service;

import java.util.List;

/**
 * Interface for exploring datasets.
 */
public interface ExploreService extends Service {

  Handle execute(String statement) throws ExploreException;

  Status getStatus(Handle handle) throws ExploreException;

  List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException;

  List<Row> nextResults(Handle handle, int size) throws ExploreException;

  Status cancel(Handle handle) throws ExploreException;

  void close(Handle handle) throws ExploreException;
}
