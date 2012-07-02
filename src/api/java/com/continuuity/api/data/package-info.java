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
 * For example, a flowlet might receive a tuple containing the userid of a given
 * user and we want to read the e-mail address for this user from the data
 * fabric.  The below example, JoinUserEmailFlowlet, performs this join inline
 * using the read operation executor and adds the email address of the user to
 * a tuple and emits it.
 * 
 * <pre>
 * public class JoinUserEmailFlowlet extends AbstractComputeFlowlet {
 *   @Override
 *   public void configure(StreamsConfigurator configurator) {
 *     // input = {(userid, byte[])}
 *     configurator.getDefaultTupleInputStream().setSchema(
 *         new TupleSchemaBuilder()
 *         .add("userid", byte[].class)
 *         .create());
 *     // output = {(userid, byte[]), (email, byte[])}
 *     configurator.getDefaultTupleOutputStream().setSchema(
 *         new TupleSchemaBuilder()
 *         .add("userid", byte[].class)
 *         .add("email", byte[].class)
 *         .create());
 *   }
 *
 *   @Override
 *   public void process(Tuple tuple, TupleContext context,
 *       OutputCollector collector) {
 *     byte [] userid = tuple.get("userid");
 *     Read read = new Read(userid, "email".getBytes());
 *     ReadOperationExecutor executor = getFlowletLaunchContext().getReadExecutor();
 *     byte [] email = executor.execute(read).get("email".getBytes());
 *     collector.emit(
 *         new TupleBuilder()
 *         .set("userid", userid)
 *         .set("email", email)
 *         .create());
 *   }
 * }
 * </pre>
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
 * As an example of performing a write operation, we will create a flowlet
 * which does the opposite of the previous example.  It will receive a tuple
 * containing a userid and email, store the email into the data fabric, and then
 * pass along a tuple containing only the userid.
 * 
 * <pre>
 * public class StoreUserEmailFlowlet extends AbstractComputeFlowlet {
 *   @Override
 *   public void configure(StreamsConfigurator configurator) {
 *     // input = {(userid, byte[]), (email, byte[])}
 *     configurator.getDefaultTupleInputStream().setSchema(
 *         new TupleSchemaBuilder()
 *         .add("userid", byte[].class)
 *         .add("email", byte[].class)
 *         .create());
 *     // output = {(userid, byte[])}
 *     configurator.getDefaultTupleOutputStream().setSchema(
 *         new TupleSchemaBuilder()
 *         .add("userid", byte[].class)
 *         .create());
 *   }
 *
 *   @Override
 *   public void process(Tuple tuple, TupleContext context,
 *       OutputCollector collector) {
 *     byte [] userid = tuple.get("userid");
 *     byte [] email = tuple.get("email");
 *     Write write = new Write(userid, "email".getBytes(), email);
 *     collector.emit(write);
 *     collector.emit(
 *         new TupleBuilder()
 *         .set("userid", userid)
 *         .create());
 *   }
 * }
 * </pre>
 * 
 * There are four types of read operations and four types of write operations.
 * 
 * In this case of many operations, two different modes of storage are
 * supported: key-value and columnar.  Key-value is simple key-to-value storage
 * while columnar is a column-oriented storage schema that allows each row to
 * contain any number of unique columns.
 * 
 * <b>Read Operations</b>
 * 
 * {@link com.continuuity.api.data.Read} is used to perform point reads on a
 * single key-value or a discrete set of columns in a single columnar row.
 * 
 * {@link com.continuuity.api.data.ReadKey} is used to perform a point read on
 * a single key of a stored key-value.
 * 
 * {@link com.continuuity.api.data.ReadColumnRange} is used to read a range of
 * columns within a single columnar row.
 * 
 * {@link com.continuuity.api.data.ReadAllkeys} is used to scan the list of all
 * keys and rows.
 * 
 * <b>Write Operations</b>
 * 
 * {@link com.continuuity.api.data.Write} is used to perform all simple
 * key-value and columnar write operations.
 * 
 * {@link com.continuuity.api.data.Delete} is used to perform all simple
 * key-value and columnar delete operations.
 * 
 * {@link com.continuuity.api.data.Increment} is used to perform an atomic
 * increment operation of a key-value or a column.
 * 
 * {@link com.continuuity.api.data.CompareAndSwap} is used to perform an atomic
 * compare-and-swap operation of a key-value or a column.
 * 
 */
package com.continuuity.api.data;