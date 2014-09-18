/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * An Application is a logical grouping of
 * {@link co.cask.cdap.api.data.stream.Stream Streams},
 * {@link co.cask.cdap.api.dataset.Dataset Datasets},
 * {@link co.cask.cdap.api.flow.Flow Flows},
 * {@link co.cask.cdap.api.procedure.Procedure Procedures},
 * and other deployable elements.
 *
 * <h1>Application</h1>
 *
 * <p>
 * Every Application must either implement the {@link co.cask.cdap.api.app.Application} 
 * interface or extend the {@link co.cask.cdap.api.app.AbstractApplication} class. 
 * Extending AbstractApplication is simpler and helps produce cleaner code.
 * </p>
 *
 * <p>
 * To create a CDAP Application, first begin by extending the
 * {@link co.cask.cdap.api.app.AbstractApplication} class, then implement its 
 * {@link co.cask.cdap.api.app.Application#configure(ApplicationConfigurer configurer, ApplicationContext context)
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
 * {@link co.cask.cdap.api.data.stream.Stream}s are the primary means for
 * pushing data into CDAP.
 * </p>
 *
 * <p>
 * See {@link co.cask.cdap.api.data.stream.Stream} for details.
 * </p>
 *
 * <h1>Dataset</h1>
 *
 * <p>
 * A {@link co.cask.cdap.api.dataset.Dataset Dataset} defines the storage and
 * retrieval of data. In addition to the several Dataset implementations CDAP
 * provides, you can also implement your own Custom Datasets.
 * </p>
 *
 * <p>
 * See {@link co.cask.cdap.api.dataset.Dataset} for details.
 * </p>
 *
 * <h1>Flow</h1>
 *
 * <p>
 * {@link co.cask.cdap.api.flow.Flow}s are user-implemented real-time stream
 * processors. A Flow contains one or more
 * {@link co.cask.cdap.api.flow.flowlet.Flowlet}s that are
 * wired into a Directed Acyclic Graph (DAG).
 * </p>
 *
 * <p>
 * In order to define a {@link co.cask.cdap.api.flow.Flow}, implement
 * the {@link co.cask.cdap.api.flow.Flow} interface and implement the
 * {@link co.cask.cdap.api.flow.Flow#configure()} method.
 * </p>
 *
 * <p>
 * See {@link co.cask.cdap.api.flow.Flow} for details.
 * </p>
 *
 * <h2>Flowlet</h2>
 *
 * <p>
 * A {@link co.cask.cdap.api.flow.flowlet.Flowlet} is a processing unit of a
 * {@link co.cask.cdap.api.flow.Flow} that defines business logic for
 * processing events received on input. Flowlets can also emit new events to the
 * output for downstream processing. In other words, Flowlets pass Data Objects
 * between one another. Each Flowlet is able to perform custom logic and
 * execute data operations for each individual data object processed.
 * </p>
 *
 * <p>
 * See {@link co.cask.cdap.api.flow.flowlet.Flowlet} for details.
 * </p>
 *
 * <h1>Procedure</h1>
 *
 * <p>
 * A {@link co.cask.cdap.api.procedure.Procedure} handles queries from external
 * systems to the CDAP instance and performs arbitrary server-side processing on demand.
 * </p>
 *
 * <p>
 * To define a {@link co.cask.cdap.api.procedure.Procedure}, implement the
 * {@link co.cask.cdap.api.procedure.Procedure} interface.
 * </p>
 *
 * <p>
 * See {@link co.cask.cdap.api.procedure.Procedure} for details.
 * </p>
 */
package co.cask.cdap.api.app;
