/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

/**
 * An Application is a logical grouping of
 * {@link com.continuuity.api.data.stream.Stream Streams},
 * {@link com.continuuity.api.data.DataSet Datasets},
 * {@link com.continuuity.api.flow.Flow Flows},
 * {@link com.continuuity.api.procedure.Procedure Procedures},
 * and other deployable elements.
 *
 * <h1>Application</h1>
 *
 * <p>
 * Every Application must either implement the {@link com.continuuity.api.app.Application} 
 * interface or extend the {@link com.continuuity.api.app.AbstractApplication} class. 
 * Extending AbstractApplication is simpler and helps produce cleaner code.
 * </p>
 *
 * <p>
 * To create a Reactor Application, first begin by extending the
 * {@link com.continuuity.api.app.AbstractApplication} class, then implement its 
 * {@link com.continuuity.api.app.Application#configure(ApplicationConfigurer configurer, ApplicationContext context)
 * configure()} method. In the configure method, specify the Application's metadata
 * (its name and description), and declare and configure each of the Application elements.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 *
 * <pre>
 * <code>
 * public class MyApp extends AbstractApplication {
 *   {@literal @}Override
 *   public void configure() {
 *     setName("myApp");
 *     setDescription("My Sample Application");
 *     addStream(new Stream("myAppStream"));
 *     createDataset("myCounters", "KeyValueTable");
 *     addFlow(new MyAppFlow());
 *     addProcedure(new MyAppQuery());
 *     addMapReduce(new MyMapReduceJob());
 *     addWorkflow(new MyAppWorkflow());
 *   }
 * }
 * </code>
 * </pre>
 * </p>
 *
 * <h1>Stream</h1>
 *
 * <p>
 * {@link com.continuuity.api.data.stream.Stream}s are the primary means for
 * pushing data into the Reactor.
 * </p>
 *
 * <p>
 * See {@link com.continuuity.api.data.stream.Stream} for details.
 * </p>
 *
 * <h1>DataSet</h1>
 *
 * <p>
 * A {@link com.continuuity.api.data.DataSet Dataset} defines the storage and
 * retrieval of data. In addition to the several Dataset implementations the Reactor
 * provides, you can also implement your own Custom Datasets.
 * </p>
 *
 * <p>
 * See {@link com.continuuity.api.data.DataSet} for details.
 * </p>
 *
 * <h1>Flow</h1>
 *
 * <p>
 * {@link com.continuuity.api.flow.Flow}s are user-implemented real-time stream
 * processors. A Flow contains one or more
 * {@link com.continuuity.api.flow.flowlet.Flowlet}s that are
 * wired into a Directed Acyclic Graph (DAG).
 * </p>
 *
 * <p>
 * In order to define a {@link com.continuuity.api.flow.Flow}, implement
 * the {@link com.continuuity.api.flow.Flow} interface and implement the
 * {@link com.continuuity.api.flow.Flow#configure()} method.
 * </p>
 *
 * <p>
 * See {@link com.continuuity.api.flow.Flow} for details.
 * </p>
 *
 * <h2>Flowlet</h2>
 *
 * <p>
 * A {@link com.continuuity.api.flow.flowlet.Flowlet} is a processing unit of a
 * {@link com.continuuity.api.flow.Flow} that defines business logic for
 * processing events received on input. Flowlets can also emit new events to the
 * output for downstream processing. In other words, Flowlets pass Data Objects
 * between one another. Each Flowlet is able to perform custom logic and
 * execute data operations for each individual data object processed.
 * </p>
 *
 * <p>
 * See {@link com.continuuity.api.flow.flowlet.Flowlet} for details.
 * </p>
 *
 * <h1>Procedure</h1>
 *
 * <p>
 * A {@link com.continuuity.api.procedure.Procedure} handles queries from external
 * systems to the Reactor and performs arbitrary server-side processing on demand.
 * </p>
 *
 * <p>
 * To define a {@link com.continuuity.api.procedure.Procedure}, implement the
 * {@link com.continuuity.api.procedure.Procedure} interface.
 * </p>
 *
 * <p>
 * See {@link com.continuuity.api.procedure.Procedure} for details.
 * </p>
 */
package com.continuuity.api.app;
