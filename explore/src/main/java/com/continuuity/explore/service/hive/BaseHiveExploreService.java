package com.continuuity.explore.service.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.hive.context.CConfCodec;
import com.continuuity.hive.context.ConfigurationUtil;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.HConfCodec;
import com.continuuity.hive.context.TxnCodec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Defines common functionality used by different HiveExploreServices. The common functionality includes
 * starting/stopping transactions, serializing configuration and saving operation information.
 */
public abstract class BaseHiveExploreService extends AbstractIdleService implements ExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHiveExploreService.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HiveConf hiveConf;

  // TODO: timeout operations
  private final ConcurrentMap<Handle, OperationInfo> handleMap = Maps.newConcurrentMap();

  private final CLIService cliService;

  protected BaseHiveExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                   CConfiguration cConf, Configuration hConf, HiveConf hiveConf) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.hiveConf = hiveConf;
    this.cliService = new CLIService();
    ContextManager.initialize(txClient, datasetFramework);
  }

  protected HiveConf getHiveConf() {
    // TODO figure out why this hive conf does not contain our env properties
    // return hiveConf;
    return new HiveConf();
  }

  protected CLIService getCliService() {
    return cliService;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", Hive13ExploreService.class.getSimpleName());
    cliService.init(getHiveConf());
    cliService.start();
    // TODO: Figure out a way to determine when cliService has started successfully - REACTOR-254
    TimeUnit.SECONDS.sleep(5);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", Hive13ExploreService.class.getSimpleName());
    cliService.stop();
  }

  @Override
  public Handle execute(String statement) throws ExploreException {
    try {
      Map<String, String> sessionConf = startSession();
      SessionHandle sessionHandle = cliService.openSession("hive", "", sessionConf);
      OperationHandle operationHandle = cliService.executeStatementAsync(sessionHandle, statement,
                                                                         ImmutableMap.<String, String>of());
      Handle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf);
      LOG.trace("Executing statement: {} with handle {}", statement, handle);
      return handle;
    } catch (Exception e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException, HandleNotFoundException {
    try {
      LOG.trace("Getting schema for handle {}", handle);
      ImmutableList.Builder<ColumnDesc> listBuilder = ImmutableList.builder();
      OperationHandle operationHandle = getOperationHandle(handle);
      if (operationHandle.hasResultSet()) {
        TableSchema tableSchema = cliService.getResultSetMetadata(operationHandle);
        for (ColumnDescriptor colDesc : tableSchema.getColumnDescriptors()) {
          listBuilder.add(new ColumnDesc(colDesc.getName(), colDesc.getTypeName(),
                                         colDesc.getOrdinalPosition(), colDesc.getComment()));
        }
      }
      return listBuilder.build();
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public void cancel(Handle handle) throws ExploreException, HandleNotFoundException {
    try {
      LOG.trace("Cancelling operation {}", handle);
      cliService.cancelOperation(getOperationHandle(handle));
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public void close(Handle handle) throws ExploreException, HandleNotFoundException {
    try {
      LOG.trace("Closing operation {}", handle);
      cliService.closeOperation(getOperationHandle(handle));
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    } finally {
      try {
        closeSession(handle);
      } finally {
        cleanUp(handle);
      }
    }
  }

  private void closeSession(Handle handle) {
    try {
      SessionHandle sessionHandle = getSessionHandle(handle);
      cliService.closeSession(sessionHandle);
    } catch (Throwable e) {
      LOG.error("Got error closing session", e);
    }
  }

  /**
   * Starts a long running transaction, and also sets up session configuration.
   * @return configuration for a hive session that contains a transaction, and serialized reactor configuration and
   * HBase configuration. This will be used by the map-reduce tasks started by Hive.
   * @throws IOException
   */
  protected Map<String, String> startSession() throws IOException {
    Map<String, String> sessionConf = Maps.newHashMap();

    Transaction tx = startTransaction();
    ConfigurationUtil.set(sessionConf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE, tx);
    ConfigurationUtil.set(sessionConf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE, cConf);
    ConfigurationUtil.set(sessionConf, Constants.Explore.HCONF_KEY, HConfCodec.INSTANCE, hConf);
    return sessionConf;
  }

  /**
   * Returns {@link OperationHandle} associated with Explore {@link Handle}.
   * @param handle explore handle.
   * @return OperationHandle.
   * @throws ExploreException
   */
  protected OperationHandle getOperationHandle(Handle handle) throws ExploreException, HandleNotFoundException {
    OperationInfo opInfo = handleMap.get(handle);
    if (opInfo == null) {
      throw new HandleNotFoundException("Invalid handle provided");
    }
    return opInfo.getOperationHandle();
  }

  /**
   * Returns {@link SessionHandle} associated with Explore {@link Handle}.
   * @param handle explore handle.
   * @return SessionHandle.
   * @throws ExploreException
   */
  protected SessionHandle getSessionHandle(Handle handle) throws ExploreException, HandleNotFoundException {
    OperationInfo opInfo = handleMap.get(handle);
    if (opInfo == null) {
      throw new HandleNotFoundException("Invalid handle provided");
    }
    return opInfo.getSessionHandle();
  }

  /**
   * Saves information associated with an Hive operation.
   * @param operationHandle {@link OperationHandle} of the Hive operation running.
   * @param sessionHandle {@link SessionHandle} for the Hive operation running.
   * @param sessionConf configuration for the session running the Hive operation.
   * @return {@link Handle} that represents the Hive operation being run.
   */
  protected Handle saveOperationInfo(OperationHandle operationHandle, SessionHandle sessionHandle,
                                     Map<String, String> sessionConf) {
    Handle handle = Handle.generate();
    handleMap.put(handle, new OperationInfo(sessionHandle, operationHandle, sessionConf));
    return handle;
  }

  /**
   * Cleans up the metadata associated with the {@link Handle}. It also closes associated transaction.
   * @param handle handle of the running Hive operation.
   */
  protected void cleanUp(Handle handle) {
    try {
      closeTransaction(handle);
    } finally {
      handleMap.remove(handle);
    }
  }

  private Transaction startTransaction() throws IOException {
    TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
    Transaction tx = txClient.startLong();
    LOG.debug("Transaction {} started.", tx);
    return tx;
  }

  private void closeTransaction(Handle handle) {
    try {
      OperationInfo opInfo = handleMap.get(handle);
      Transaction tx = ConfigurationUtil.get(opInfo.getSessionConf(),
                                             Constants.Explore.TX_QUERY_KEY,
                                             TxnCodec.INSTANCE);
      LOG.debug("Closing transaction {} for handle {}", tx, handle);

      TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
      // Transaction doesn't involve any changes. We still commit it to take care of any side effect changes that
      // SplitReader may have.
      if (!(txClient.canCommit(tx, ImmutableList.<byte[]>of()) && txClient.commit(tx))) {
        txClient.abort(tx);
        LOG.info("Aborting transaction: {}", tx);
      }
    } catch (Throwable e) {
      LOG.error("Got exception while closing transaction.", e);
    }
  }

  /**
  * Helper class to store information about a Hive operation in progress.
  */
  static class OperationInfo {
    private final SessionHandle sessionHandle;
    private final OperationHandle operationHandle;
    private final Map<String, String> sessionConf;

    OperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                  Map<String, String> sessionConf) {
      this.sessionHandle = sessionHandle;
      this.operationHandle = operationHandle;
      this.sessionConf = sessionConf;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public OperationHandle getOperationHandle() {
      return operationHandle;
    }

    public Map<String, String> getSessionConf() {
      return sessionConf;
    }
  }
}
