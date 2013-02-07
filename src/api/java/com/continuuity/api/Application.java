/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

/**
 * Application is a logical grouping of Streams, Processors, DataSets
 * and Procedures.
 *
 * <p>
 * To create an {@code Application}, you have to implement the interface and it's
 * {@link #configure()} method. This allows you to specify the metadata of the application, adding
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.flow.Flow Flows},
 * {@link com.continuuity.api.data.DataSet DataSets} and
 * {@link com.continuuity.api.procedure.Procedure Procedures} that are part of the application
 * </p>
 *
 * @see com.continuuity.api.data.stream.Stream Stream
 * @see com.continuuity.api.flow.Flow Flow
 * @see com.continuuity.api.data.DataSet DataSet
 * @see com.continuuity.api.procedure.Procedure Procedure
 */
public interface Application {
  /**
   * Configures the {@link Application} by returning an {@link ApplicationSpecification}
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  ApplicationSpecification configure();
}