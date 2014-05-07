/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

/**
 * A temporary place for hosting MDS access logic for streams.
 */
// TODO: The whole access pattern to MDS needs to be rethink, as we are now moving towards SOA and multiple components
// needs to access MDS.
public interface StreamMetaStore {

  /**
   * Adds a stream to the meta store.
   */
  void addStream(String accountId, String streamName) throws Exception;

  /**
   * Removes a stream from the meta store.
   */
  void removeStream(String accountId, String streamName) throws Exception;

  /**
   * Checks if a stream exists in the meta store.
   */
  boolean streamExists(String accountId, String streamName) throws Exception;
}
