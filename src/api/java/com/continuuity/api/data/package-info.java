/**
 * This package contains all public Data Access APIs.
 *
 * All data access is performed through the creation and execution of
 * {@link com.continuuity.api.data.Operation}s, of which there are two primary
 * types, {@link com.continuuity.api.data.ReadOperation} and
 * {@link com.continuuity.api.data.WriteOperation}.
 * 
 * Operations are executed through two primary pathways: a synchronous read path
 * and an asynchronous write path.
 * 
 * Read operations are performed one-at-a-time and inline within a flowlet
 * via the {@link com.continuuity.api.data.ReadOperationExecutor}.  The result
 * of the operation is returned from the executor or is available via
 * {@link com.continuuity.api.data.ReadOperation#getResult()}.
 * 
 * Write operations are performed within a transaction and as an asynchronous
 * batch at the end of every flowlet process call.  Writes are added to this
 * batch through {@link com.continuuity.api.flow.flowlet.OutputCollector#emit(WriteOperation)}.
 * 
 * This batch will either completely succeed or completely fail, the database
 * will never be left in an inconsistent state.  If the batch is successful,
 * all the changes are committed permanently and the flowlet will receive an
 * {@link com.continuuity.api.flow.flowlet.Callback#onSuccess()} callback.  If
 * the batch is not successfully transacted (a conditional operation could fail
 * or some other kind of non-retryable error may occur), all operations in the
 * batch are aborted, the database remains unchanged, and the flowlet will
 * receive an {@link com.continuuity.api.flow.flowlet.Callback#onFailure()}
 * callback.
 * 
 * If the batch of writes is successful, 
 * 
 * 
 * In order to define a new flow programtically, the Flow interface needs to
 * be implemented. The implementation would configure the flow using the
 * {@link com.continuuity.api.flow.FlowSpecifier}. Following is
 * a simple example of how a flow could be defined programtically.
 *
 * <pre>
 *   public class MyFlowWithSource implements Flow {
 *     public void configure(FlowSpecifier specifier) {
 *       // Specify meta data of a flow.
 *       specifier.name("correct");
 *       specifier.email("me@continuuity.com");
 *       specifier.namespace("com.continuuity");
 *
 *       // Specify flowlets
 *       specifier.flowlet("source", SimpleSourceFlowlet.class , 1);
 *       specifier.flowlet("compute", SimpleComputeFlowlet.class, 1);
 *       specifier.flowlet("sink", SimpleSinkFlowlet.class, 1);
 *
 *       // Specify connections between flowlets.
 *       specifier.connection("source", "compute");
 *       specifier.connection("compute", "sink");
 *     }
 *   }
 * </pre>
 *
 * The above code defines three flowlets using
 * {@link com.continuuity.api.flow.FlowSpecifier#flowlet(String, Class, int)} API
 *
 * <ul>
 *   <li>SimpleSourceFlowlet</li>
 *   <li>SimpleComputeFlowlet &</li>
 *   <li>SimpleSinkFlowlet</li>
 * </ul>
 *
 * Once, the flowlets are defined, you provide the topology of how they are connected. In the
 * above example following are the interconnects
 * <ul>
 *   <li>SimpleSourceFlowlet to SimpleComputeFlowlet</li>
 *   <li>SimpleComputeFlowlet to SimpleSinkFlowlet</li>
 * </ul>
 *
 * <p>
 * Flow merely defines how the topology of the flow would look like. Next step is to provide the
 * the Flow to a FlowRunner to start and manage while it's running. {@link com.continuuity.api.flow.FlowRunner} can be used
 * to start a flow. Following is an example of how to start a flow in-memory.
 *
 * <pre>
 *   FlowRunner runner = new InMemoryFlowRunner(new MyFlowWithSource());
 *   try {
 *     runner.start();
 *     runner.monitor(); // blocks.
 *   } catch (FlowRunnerException e){
 *     e.printStackTrace();
 *   }
 * </pre>
 *
 * </p>
 *
 */
package com.continuuity.api.data;