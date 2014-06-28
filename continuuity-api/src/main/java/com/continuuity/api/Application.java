/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

/**
 * A Continuuity Reactor Application is a logical ordered grouping of
 * Streams, DataSets, Flows, Procedures, MapReduce jobs and Workflows.
 *
 * <p>
 * To create an {@code Application}, implement the Application interface and its
 * {@link #configure()} method, specifing the metadata of the application
 * and adding the
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.data.DataSet DataSets},
 * {@link com.continuuity.api.flow.Flow Flows},
 * {@link com.continuuity.api.procedure.Procedure Procedures},
 * {@link com.continuuity.api.mapreduce.MapReduce MapReduce} jobs and
 * {@link com.continuuity.api.workflow.Workflow Workflows} that are part of
 * the Application.
 * </p>
 * <p>
 *   Example Application:
 * </p>
 * <pre>
 *   <code>
 *     public class PurchaseApp implements Application {
 *       {@literal @}Override
 *       public ApplicationSpecification configure() {
 *         try {
 *           return ApplicationSpecification.Builder.with()
 *             .setName("PurchaseHistory")
 *             .setDescription("Purchase history app")
 *             .withStreams()
 *               .add(new Stream("purchaseStream"))
 *             .withDataSets()
 *               .add(new ObjectStore{@literal <}PurchaseHistory>("history", PurchaseHistory.class))
 *               .add(new ObjectStore{@literal <}Purchase>("purchases", Purchase.class))
 *             .withFlows()
 *               .add(new PurchaseFlow())
 *             .withProcedures()
 *               .add(new PurchaseQuery())
 *             .noMapReduce()
 *             .withWorkflows()
 *               .add(new PurchaseHistoryWorkflow())
 *             .build();
 *         } catch (UnsupportedTypeException e) {
 *           throw new RuntimeException(e);
 *         }
 *       }
 *     }
 *   </code>
 * </pre>
 * <p>
 * See the <i>Continuuity Reactor Programming Guide</i> and
 * the Continuuity Reactor example applications.
 * </p>
 *
 * @see com.continuuity.api.data.stream.Stream Stream
 * @see com.continuuity.api.data.DataSet DataSet
 * @see com.continuuity.api.flow.Flow Flow
 * @see com.continuuity.api.procedure.Procedure Procedure
 * @see com.continuuity.api.mapreduce.MapReduce MapReduce
 * @see com.continuuity.api.workflow.Workflow Workflow
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.app.Application}
 */
@Deprecated
public interface Application {
  /**
   * Configures the {@link Application} by returning an {@link ApplicationSpecification}.
   *
   * @return An instance of {@link ApplicationSpecification}.
   */
  ApplicationSpecification configure();
}
