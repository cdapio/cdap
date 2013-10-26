/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * Interfaces for {@link com.continuuity.api.flow.Flow Flows},
 * {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets},
 * {@link com.continuuity.api.data.stream.Stream Streams}, 
 * {@link com.continuuity.api.data.DataSet DataSets}, 
 * {@link com.continuuity.api.procedure.Procedure Procedures}, 
 * {@link com.continuuity.api.mapreduce.MapReduce}, and
 * {@link com.continuuity.api.workflow.Workflow Workflows}.
 *
 * <h1>Application</h1>
 * An application is a logical grouping of:
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.data.DataSet DataSets},
 * {@link com.continuuity.api.flow.Flow Flows} and
 * {@link com.continuuity.api.procedure.Procedure Procedures}, and so forth that are deployable.
 *
 * <p>
 *   In order to create a Reactor application, begin by implementing
 *   the {@link com.continuuity.api.Application} interface.
 *   When you implement the {@link com.continuuity.api.Application#configure()} method,
 *   you create an {@link com.continuuity.api.ApplicationSpecification}
 *   that defines and/or creates all of the components of an {@link com.continuuity.api.Application}.
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
 *             .withStreams()
 *               .add(new Stream("text"))
 *               .add(new Stream("log"))
 *             .withDataSets()
 *               .add(new KeyValueTable("mytable"))
 *               .add(new SimpleTimeseriesTable("tstable"))
 *             .withFlows()
 *               .add(new MyFirstFlow())
 *               .add(new LogProcessFlow())
 *             .withProcedures()
 *               .add(new KeyValueLookupProcedure())
 *               .add(new LogProcedure())
 *             .build();
 *     }
 *   }
 *   </pre>
 * </p>
 *
 * <h1>Flow</h1>
 * {@link com.continuuity.api.flow.Flow}s are user-implemented real-time stream processors. A flow contains  
 * one or more {@link com.continuuity.api.flow.flowlet.Flowlet}s that are 
 * wired into a Directed Acyclic Graph (DAG).  
 * <p>
 *   In order to define a {@link com.continuuity.api.flow.Flow}, implement 
 *   the {@link com.continuuity.api.flow.Flow} interface and implement the 
 *   {@link com.continuuity.api.flow.Flow#configure()} method.
 * </p>
 * <p>
 *   See {@link com.continuuity.api.flow.Flow} for more details.
 * </p>
 *
 * <h2>Flowlet</h2>
 * A {@link com.continuuity.api.flow.flowlet.Flowlet} is a processing unit of a {@link com.continuuity.api.flow.Flow} 
 * that defines business logic for processing events received on input. Flowlets can also emit new events on the output 
 * for downstream processing. In other words, flowlets pass Data Objects between one another. 
 * Each flowlet is able to perform custom logic and execute data operations for each individual data object processed.
 * <p>
 *   See {@link com.continuuity.api.flow.flowlet.Flowlet} for more details.
 * </p>
 *
 * <h1>Procedure</h1>
 * A {@link com.continuuity.api.procedure.Procedure} handles queries from external systems to the Reactor 
 * and performs arbitrary server-side processing on demand.
 * <p>
 *   To define a {@link com.continuuity.api.procedure.Procedure}, implement the 
 *   {@link com.continuuity.api.procedure.Procedure} interface.
 * </p>
 * <p>
 *   See {@link com.continuuity.api.procedure.Procedure} for more details.
 * </p>
 *
 * <h1>DataSet</h1>
 * A {@link com.continuuity.api.data.DataSet} defines the way you store and retrieve data. The Reactor provides 
 * several {@link com.continuuity.api.data.DataSet} implementations and you can also implement your own.
 * <p>
 *   See {@link com.continuuity.api.data.DataSet} for more details.
 * </p>
 *
 * <h1>Stream</h1>
 * {@link com.continuuity.api.data.stream.Stream}s are the primary means for pushing data into the Reactor.
 * <p>
 *   See {@link com.continuuity.api.data.stream.Stream} for more details.
 * </p>
 */
package com.continuuity.api;
