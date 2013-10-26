/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

/**
 * An Application is a logical grouping of Streams, DataSets, Flows, MapReduce, Procedures, and Workflows.
 *
 * <p>
 * To create an {@code Application}, implement the Application interface and its
 * {@link #configure()} method. 
 * 
 * 
 * This allows you to specify the metadata of the application, adding the 
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.flow.Flow Flows}, 
 * {@link com.continuuity.api.mapreduce.MapReduce MapReduce},
 * {@link com.continuuity.api.data.DataSet DataSets},
 * {@link com.continuuity.api.procedure.Procedure Procedures} and 
 * {@link com.continuuity.api.workflow.Workflow Workflows} that are part of the application.
 * </p>
 *
 * <p>
 * See the <i>Continuuity Reactor Developer Guide</i> and the Reactor example applications.
 *
 * <p>
 * @see com.continuuity.api.data.stream.Stream Stream
 * @see com.continuuity.api.flow.Flow Flow
 * @see com.continuuity.api.mapreduce.MapReduce MapReduce
 * @see com.continuuity.api.data.DataSet DataSet
 * @see com.continuuity.api.procedure.Procedure Procedure
 * @see com.continuuity.api.workflow.Workflow Workflow
 * </p>
 */
public interface Application {
  /**
   * Configures the {@link Application} by returning an {@link ApplicationSpecification}.
   *
   * @return An instance of {@link ApplicationSpecification}.
   */
  ApplicationSpecification configure();
}
