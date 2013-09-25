/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * This package contains Continuuity Reactor API interfaces:  {@link com.continuuity.api.flow.Flow Flows},
 * {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets},
 * {@link com.continuuity.api.data.stream.Stream Streams}, {@link com.continuuity.api.data.DataSet DataSets} and
 * {@link com.continuuity.api.procedure.Procedure Procedures}.
 *
 * <h1>Application</h1>
 * An application is a logical grouping of:
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.data.DataSet DataSets},
 * {@link com.continuuity.api.flow.Flow Flows} and
 * {@link com.continuuity.api.procedure.Procedure Procedures} that are deployable.
 *
 * <p>
 *   In order to create a Reactor application, begin by implementing
 *   the {@link com.continuuity.api.Application} interface.
 *   When you implement the {@link com.continuuity.api.Application#configure()} method,
 *   you create an {@link com.continuuity.api.ApplicationSpecification}
 *   that defines and/or creates all the components of an {@link com.continuuity.api.Application}.
 * </p>
 *
 * <p>
 * Example usage:
 *   <pre>
 *   public MyApplication implements Application {
 *
 *     public ApplicationSpecification configure() {
 *
 *       return ApplicationSpecification.Builder.with()
 *             .setName("myFirstApp")
 *             .setDescription("This is my first application")
 *             .withStreams().add(new Stream("text"))
 *                           .add(new Stream("log"))
 *             .withDataSets().add(new KeyValueTable("mytable"))
 *                            .add(new SimpleTimeseriesTable("tstable"))
 *             .withFlows().add(new MyFirstFlow())
 *                         .add(new LogProcessFlow())
 *             .withProcedures().add(new KeyValueLookupProcedure())
 *                              .add(new LogProcedure())
 *             .build();
 *     }
 *   }
 *   </pre>
 * </p>
 *
 * <h1>Flow</h1>
 * A {@link com.continuuity.api.flow.Flow} is type of Processor that enables real time processing of events.
 * A {@link com.continuuity.api.flow.Flow} is set of {@link com.continuuity.api.flow.flowlet.Flowlet}s
 * connected by queues.
 * <p>
 *   In order to define a {@link com.continuuity.api.flow.Flow}, you need to implement
 *   the {@link com.continuuity.api.flow.Flow} interface and implement the
 *   {@link com.continuuity.api.flow.Flow#configure()} method.
 * </p>
 * <p>
 *   See {@link com.continuuity.api.flow.Flow} for more details.
 * </p>
 *
 * <h2>Flowlet</h2>
 * {@link com.continuuity.api.flow.flowlet.Flowlet} is a processing unit of a {@link com.continuuity.api.flow.Flow}
 * that defines business logic for processing events received on input and also can emit new events on the output
 * for downstream processing.
 * <p>
 *   See {@link com.continuuity.api.flow.flowlet.Flowlet} for more details.
 * </p>
 *
 * <h1>Procedure</h1>
 * A {@link com.continuuity.api.procedure.Procedure} is for handling queries from external systems to the AppFabric
 * and performing arbitrary server-side processing on-demand.
 * <p>
 *   To define a {@link com.continuuity.api.procedure.Procedure}, you need to implement the
 *   {@link com.continuuity.api.procedure.Procedure} interface.
 * </p>
 * <p>
 *   See {@link com.continuuity.api.procedure.Procedure} for more details.
 * </p>
 *
 * <h1>Dataset</h1>
 * {@link com.continuuity.api.data.DataSet} defines the way you store and retrieve data. The AppFabric provides
 * several {@link com.continuuity.api.data.DataSet} implementations and you could also implement your own.
 * <p>
 *   See {@link com.continuuity.api.data.DataSet} for more details.
 * </p>
 *
 * <h1>Stream</h1>
 * {@link com.continuuity.api.data.stream.Stream} are the primary means for pushing data into the AppFabric.
 * <p>
 *   See {@link com.continuuity.api.data.stream.Stream} for more details.
 * </p>
 */
package com.continuuity.api;
