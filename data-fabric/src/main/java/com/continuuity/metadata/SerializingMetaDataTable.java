package com.continuuity.metadata;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.RetryStrategies;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionConflictException;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * An implementation of MetaDataTable that serializes every entry into a byte array and stores that in a column.
 */
public class SerializingMetaDataTable implements MetaDataTable {

  private static final Logger LOG = LoggerFactory.getLogger(SerializingMetaDataTable.class);

  private final TransactionSystemClient txClient;
  private final DataSetAccessor datasetAccessor;

  // To avoid the overhead of creating new serializer, table client and tx executor for every call,
  // we keep a them for each thread in a thread local structure.
  ThreadLocal<PerThread> threadLocal = new ThreadLocal<PerThread>() {
    @Override
    protected PerThread initialValue() {
      OrderedColumnarTable table;
      try {
        table = datasetAccessor.getDataSetClient(META_DATA_TABLE_NAME,
                                                 OrderedColumnarTable.class,
                                                 DataSetAccessor.Namespace.SYSTEM);
      } catch (Exception e) {
        LOG.error("Failed to get a dataset client for meta data table.", e);
        throw Throwables.propagate(e);
      }

      // NOTE: this code is old and needs major redoing. We don't want to break what was working for long time, hence
      //       using no retries the most plain way
      TransactionExecutor executor =
        new DefaultTransactionExecutor(txClient, ImmutableList.of((TransactionAware) table),
                                       RetryStrategies.noRetries());
      return new PerThread(new MetaDataSerializer(), table, executor);
    }
  };

  private MetaDataSerializer getSerializer() {
    return threadLocal.get().getSerializer();
  }

  private OrderedColumnarTable getMetaTable() {
    return threadLocal.get().getTable();
  }

  private TransactionExecutor getTransactionExecutor() {
    return threadLocal.get().getExecutor();
  }

  @Inject
  public SerializingMetaDataTable(TransactionSystemClient txClient, DataSetAccessor accessor) {
    this.txClient = txClient;
    this.datasetAccessor = accessor;

    // ensure the meta data table exists
    try {
      DataSetManager tableManager = accessor.getDataSetManager(OrderedColumnarTable.class,
                                                               DataSetAccessor.Namespace.SYSTEM);
      if (!tableManager.exists(META_DATA_TABLE_NAME)) {
        tableManager.create(META_DATA_TABLE_NAME);
      }
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void add(OperationContext context, MetaDataEntry entry) throws OperationException {
    add(context, entry, true); // resolve = true
  }

  @Override
  public void add(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    write(context, null, entry, false, resolve); // expected = null, isUpdate = false
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry) throws OperationException {
    update(context, entry, true); // resolve = true
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    write(context, null, entry, true, resolve); // expected = null, isUpdate = true
  }

  @Override
  public void swap(OperationContext context, MetaDataEntry expected, MetaDataEntry entry) throws OperationException {
    write(context, expected, entry, true, true); // update = true, resolve = true
  }

  /**
   * Common method to implement add, update and swap. Takes a new entry and writes it to the meta data store.
   * @param expected If non-null, this method will only succeed if the current entry for the same meta data matches
   *                 this entry. This can be used for optimistic concurrency control.
   * @param entry The entry to write.
   * @param isUpdate Whether this is an update or an insert. If true, and there is already an existing entry for the
   *                 same meta data, this method will throw an exception.
   * @param resolve Whether to resolve conflicts. This can happen in three ways:
   *                <ol>
   *                  <li>the transaction fails with write conflict, but the conflicting transaction wrote the same
   *                    value</li>
   *                  <li>for an insert (isUpdate = false), if there is already a value, but it matches the value
   *                    to write</li>
   *                  <li>for a swap, if the existing value does not match expected but matches the value to
   *                    write</li>
   *                </ol>
   *                All three cases happen if multiple clients attempt the same update concurrently.
   * @throws OperationException with status code:
   * <ul>
   *   <li>ENTRY_EXISTS if isUpdate is false and an entry already exists</li>
   *   <li>ENTRY_NOT_FOUND if isUpdate is true and there is no existing entry</li>
   *   <li>ENTRY_DOES_NOT_MATCH if expected is non-null and does not match the existing entry</li>
   *   <li>WRITE_CONFLICT if the transaction fails with a write conflict</li>
   * </ul>
   */
  private void write(@SuppressWarnings("unused") OperationContext context,
                     final MetaDataEntry expected, final MetaDataEntry entry,
                     final boolean isUpdate, final boolean resolve)
    throws OperationException {

    Preconditions.checkNotNull(entry, "entry cannot be null");
    final TransactionExecutor executor = getTransactionExecutor();

    for (int tries = 0; tries <= DEFAULT_RETRIES_ON_CONFLICT; tries++) {
      try {
        executor.execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            doWrite(expected, entry, isUpdate, resolve);
          }
        });
        return;
      } catch (TransactionConflictException e) {
        // attempt conflict resolution: did someone else attempt to write the same value?
        if (resolve && entry.equals(
          get(context, entry.getAccount(), entry.getApplication(), entry.getType(), entry.getId()))) {
          return;
        }
        // someone else wrote something else... retry
      } catch (TransactionFailureException e) {
        // some other problem
        throw propagateException(e);

      } catch (InterruptedException e) {
        // should NEVER happen, since using NoRetryStrategy in tx executor
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }
    // we can only reach this point if there is a conflict and we have retried too many times
    throw new OperationException(StatusCode.WRITE_CONFLICT, "Conflicting meta data writes.");
  }

  private void doWrite(MetaDataEntry expected, MetaDataEntry entry, boolean update, boolean resolve) throws Exception {

    final byte[] row = makeRowKey(entry.getAccount());
    final byte[] column = makeColumnKey(entry);
    final byte[] bytes = getSerializer().serialize(entry);

    Map<byte[], byte[]> result = getMetaTable().get(row, new byte[][] { column });
    byte[] bytesRead = result.isEmpty() ? null : result.get(column);

    // if creating a new entry, and there is an existing entry, deal with it
    if (!update && bytesRead != null) {
      if (resolve && Arrays.equals(bytes, bytesRead)) {
        // a value exists but it is identical with the one to write
        return;
      }
      // a different value already exists
      String message = String.format("Meta data entry for %s already exists.", entry);
      LOG.debug(message);
      throw new OperationException(StatusCode.ENTRY_EXISTS, message);
    }

    if (update) {
      if (bytesRead == null) {
        // update expects a value to update, but none is there
        String message = String.format("Meta data entry for %s does not exist.", entry);
        LOG.debug(message);
        throw new OperationException(StatusCode.ENTRY_NOT_FOUND, message);
      }
      if (expected != null) {
        byte[] expectedBytes = getSerializer().serialize(expected);
        // compare with expected
        if (!Arrays.equals(bytesRead, expectedBytes)) {
          if (resolve && Arrays.equals(bytesRead, bytes)) {
            // matches the new value -> no need to write
            return;
          }
          String message = String.format("Existing meta data entry " + "for %s does not match expected entry.", entry);
          LOG.trace(message);
          // existing entry is there but does not match expected entry
          throw new OperationException(StatusCode.ENTRY_DOES_NOT_MATCH, message);
        }
      }
    }

    // write the new value, if someone else also writes at the same time, this will cause a conflict
    getMetaTable().put(row, new byte[][] { column }, new byte[][] { bytes });
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, String value, int retries)
    throws OperationException {

    Preconditions.checkNotNull(field, "field cannot be null");
    Preconditions.checkArgument(!field.isEmpty(), "field cannot be empty");

    updateField(context, account, application, type, id, field, null, null, null, value, null, retries, false);
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, byte[] value, int retries)
    throws OperationException {

    Preconditions.checkNotNull(field, "field cannot be null");
    Preconditions.checkArgument(!field.isEmpty(), "field cannot be empty");

    updateField(context, account, application, type, id, null, field, null, null, null, value, retries, false);
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, String old, String value, int retries)
    throws OperationException {

    Preconditions.checkNotNull(field, "field cannot be null");
    Preconditions.checkArgument(!field.isEmpty(), "field cannot be empty");

    updateField(context, account, application, type, id, field, null, old, null, value, null, retries, true);
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, byte[] old, byte[] value, int retries)
    throws OperationException {

    Preconditions.checkNotNull(field, "field cannot be null");
    Preconditions.checkArgument(!field.isEmpty(), "field cannot be empty");

    updateField(context, account, application, type, id, null, field, null, old, null, value, retries, true);
  }

  private void updateField(@SuppressWarnings("unused") OperationContext context,
                           String account, String application, String type, String id,
                           final String textField, final String binField, final String textOld, final byte[] binOld,
                           final String textValue, final byte[] binValue, int retryAttempts,
                           final boolean doCompareAndSwap)
    throws OperationException {

    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkNotNull(id, "id cannot be null");
    Preconditions.checkArgument(!id.isEmpty(), "id cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkArgument(!type.isEmpty(), "type cannot be empty");
    Preconditions.checkArgument(textField != null || binField != null, "Only one of textField or binField may be null");

    final byte[] row = makeRowKey(account);
    final byte[] column = makeColumnKey(application, type, id);

    final TransactionExecutor executor = getTransactionExecutor();
    final int numRetries = retryAttempts < 0 ? DEFAULT_RETRIES_ON_CONFLICT : retryAttempts;

    for (int tries = 0; tries <= numRetries; tries++) {
      try {
        executor.execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            doUpdateField(row, column, textField, binField, textOld, binOld, textValue, binValue, doCompareAndSwap);
          }
        });
        return;
      } catch (TransactionConflictException e) {
        // conflict... try again
      } catch (TransactionFailureException e) {
        // some other problem
        throw propagateException(e);

      } catch (InterruptedException e) {
        // should NEVER happen, since using NoRetryStrategy in tx executor
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }
    // we can only reach this point if there is a conflict and we have retried too many times
    throw new OperationException(StatusCode.WRITE_CONFLICT, "Conflicting meta data writes.");
  }

  void doUpdateField(byte[] row, byte[] column,
                     String textField, String binField,
                     String textOld, byte[] binOld,
                     String textValue, byte[] binValue, boolean doCompareAndSwap) throws Exception {

    // read meta data entry
    Map<byte[], byte[]> result = getMetaTable().get(row, new byte[][] { column });

    // throw exception if not existing
    if (result.isEmpty()) {
      throw new OperationException(StatusCode.ENTRY_NOT_FOUND, "Meta data entry does not exist.");
    }

    // get the raw (serialized) bytes of the entry
    byte[] bytes = result.get(column);
    if (bytes == null) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Meta data entry is null.");
    }

    // de-serialize the entry
    MetaDataEntry entry = getSerializer().deserialize(bytes);

    // in case of compare-and-swap, check the existing value
    if (doCompareAndSwap) {
      if (textField != null) {
        String existingValue = entry.getTextField(textField);
        if ((textOld == null && existingValue != null) || (textOld != null && !textOld.equals(existingValue))) {
          throw new OperationException(StatusCode.ENTRY_DOES_NOT_MATCH,
                                       "Existing field value does not match expected value");
        }
      } else if (binField != null) {
        byte[] existingValue = entry.getBinaryField(binField);
        if ((binOld == null && existingValue != null) || !Arrays.equals(binOld, existingValue)) {
          throw new OperationException(StatusCode.ENTRY_DOES_NOT_MATCH,
                                       "Existing field value does not match expected value");
        }
      }
    }

    // update the field
    if (textField != null) {
      entry.addField(textField, textValue);
    } else {
      entry.addField(binField, binValue);
    }

    // re-serialize
    byte[] newBytes = getSerializer().serialize(entry);

    // write the new value. this may conflict with other concurrent updates
    getMetaTable().put(row, new byte[][] { column }, new byte[][] { newBytes });
  }

  @Override
  public MetaDataEntry get(OperationContext context, String account, String application, String type, String id)
    throws OperationException {

    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkNotNull(id, "id cannot be null");
    Preconditions.checkArgument(!id.isEmpty(), "id cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkArgument(!type.isEmpty(), "type cannot be empty");

    final byte[] row = makeRowKey(account);
    final byte[] column = makeColumnKey(application, type, id);

    try {
      return getTransactionExecutor().execute(new TransactionExecutor.Function<Object, MetaDataEntry>() {

        @Override
        public MetaDataEntry apply(Object input) throws Exception {

          Map<byte[], byte[]> result = getMetaTable().get(row, new byte[][] { column });
          if (result.isEmpty()) {
            return null;
          }
          byte[] bytes = result.get(column);
          return bytes == null ? null : getSerializer().deserialize(bytes);
        }
      }, null);

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void delete(OperationContext context, String account, String application, String type, String id)
    throws OperationException {

    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkNotNull(id, "id cannot be null");
    Preconditions.checkArgument(!id.isEmpty(), "id cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkArgument(!type.isEmpty(), "type cannot be empty");

    final byte[] row = makeRowKey(account);
    final byte[] column = makeColumnKey(application, type, id);

    try {
      getTransactionExecutor().execute(new TransactionExecutor.Subroutine() {

        @Override
        public void apply() throws Exception {
          getMetaTable().delete(row, new byte[][] { column });
        }
      });

    } catch (TransactionConflictException e) {
      // conflict... report correct status
      throw new OperationException(StatusCode.WRITE_CONFLICT, "Conflicting meta data delete.");

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void delete(String accountId, List<MetaDataEntry> entries)
    throws OperationException {

    final byte[] row = makeRowKey(accountId);
    final byte[][] cols = new byte[entries.size()][];

    for (int i = 0; i < entries.size(); i++) {
      cols[i] = makeColumnKey(entries.get(i));
    }

    try {
      getTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          getMetaTable().delete(row, cols);
        }
      });

    } catch (TransactionConflictException e) {
      // conflict... report correct status
      throw new OperationException(StatusCode.WRITE_CONFLICT, "Conflicting meta data delete.");

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<MetaDataEntry> list(OperationContext context, String account,
                                  final String application, final String type,
                                  final Map<String, String> fields)
    throws OperationException {

    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkArgument(!type.isEmpty(), "type cannot be empty");

    final byte[] row = makeRowKey(account);
    final byte[] start = startColumnKey(application, type);
    final byte[] stop = stopColumnKey(application, type);

    try {
      return getTransactionExecutor().execute(new TransactionExecutor.Function<Object, List<MetaDataEntry>>() {
        @Override
        public List<MetaDataEntry> apply(Object input) throws Exception {
          return doList(row, start, stop, application, type, fields);
        }
      }, null);

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<MetaDataEntry> list(final OperationContext context, final String account,
                                  final String application, final String type,
                                  final String startId, final String stopId, final int count)
    throws OperationException {
    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkArgument(!type.isEmpty(), "type cannot be empty");
    Preconditions.checkNotNull(startId, "start id cannot be null");
    Preconditions.checkArgument(!startId.isEmpty(), "start id cannot be empty");
    Preconditions.checkNotNull(stopId, "stop id cannot be null");
    Preconditions.checkArgument(!stopId.isEmpty(), "stop id cannot be empty");

    final byte[] row = makeRowKey(account);
    final byte[] start = makeColumnKey(application, type, startId);
    final byte[] stop = makeColumnKey(application, type, stopId);

    try {
      return getTransactionExecutor().execute(new TransactionExecutor.Function<Object, List<MetaDataEntry>>() {
        @Override
        public List<MetaDataEntry> apply(Object input) throws Exception {
          return doList(row, start, stop, count);
        }
      }, null);

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  private List<MetaDataEntry> doList(byte[] row, byte[] start, byte[] stop,
                                     String application, String type, Map<String, String> fields)
    throws Exception {

    Map<byte[], byte[]> result = getMetaTable().get(row, start, stop, Integer.MAX_VALUE);
    if (result.isEmpty()) {
      return Collections.emptyList();
    }
    List<MetaDataEntry> entries = Lists.newArrayList();
    for (byte[] bytes : result.values()) {
      MetaDataEntry meta = getSerializer().deserialize(bytes);
      if (!type.equals(meta.getType())) {
        continue;
      }
      if (application != null && !application.equals(meta.getApplication())) {
        continue;
      }

      if (fields != null) {
        boolean match = true;
        for (Map.Entry<String, String> field : fields.entrySet()) {
          if (field.getValue() == null) {
            if (!meta.getTextFields().contains(field.getKey())) {
              match = false;
              break;
            }
          } else if (!field.getValue().equals(meta.getTextField(field.getKey()))) {
            match = false;
            break;
          }
        }
        if (!match) {
          continue;
        }
      }
      entries.add(meta);
    }
    return entries;
  }

  private List<MetaDataEntry> doList(byte[] row, byte[] start, byte[] stop, int count)
    throws Exception {
    Map<byte[], byte[]> result = getMetaTable().get(row, start, stop, count);
    List<MetaDataEntry> entries = Lists.newArrayList();
    for (byte[] bytes : result.values()) {
      MetaDataEntry meta = getSerializer().deserialize(bytes);
      entries.add(meta);
    }
    return entries;
  }


  @Override
  public void clear(OperationContext context, String account, final String application) throws OperationException {

    Preconditions.checkNotNull(account, "account cannot be null");
    Preconditions.checkArgument(!account.isEmpty(), "account cannot be empty");
    Preconditions.checkArgument(application == null || !application.isEmpty(), "application cannot be empty");

    final byte[] row = makeRowKey(account);

    try {
      getTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          doClear(row, application);
        }
      });
    } catch (TransactionConflictException e) {
      // conflict... report correct status
      throw new OperationException(StatusCode.WRITE_CONFLICT, "Conflicting meta data delete.");

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  private void doClear(byte[] row, String application) throws Exception {

    Map<byte[], byte[]> result = getMetaTable().get(row, null, null, Integer.MAX_VALUE);
    if (result.isEmpty() || result.isEmpty()) {
      return; // nothing to clear
    }
    byte[][] columns;
    Set<byte[]> colSet = result.keySet();
    if (application == null) {
      columns = colSet.toArray(new byte[colSet.size()][]);
    } else {
      List<byte[]> cols = Lists.newArrayList();
      for (byte[] colKey : colSet) {
        if (application.equals(extractApplication(colKey))) {
          cols.add(colKey);
        }
      }
      columns = cols.toArray(new byte[cols.size()][]);
    }
    getMetaTable().delete(row, columns);
  }

  @Override
  public Collection<String> listAccounts(OperationContext context) throws OperationException {
    try {
      return getTransactionExecutor().execute(new TransactionExecutor.Function<Object, Collection<String>>() {
        @Override
        public List<String> apply(Object input) throws Exception {
          return doListAccounts();
        }
      }, null);

    } catch (TransactionFailureException e) {
      // some other problem
      throw propagateException(e);

    } catch (InterruptedException e) {
      // should NEVER happen, since using NoRetryStrategy in tx executor
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  private List<String> doListAccounts() throws Exception {

    Scanner scanner = getMetaTable().scan(null, null);
    List<String> accounts = Lists.newArrayList();
    try {
      while (true) {
        ImmutablePair<byte[], Map<byte[], byte[]>> row = scanner.next();
        if (row == null) {
          break;
        }
        accounts.add(extractAccountFromRowKey(row.getFirst()));
      }
    } finally {
      scanner.close();
    }
    return accounts;
  }

  @Override
  public void upgrade() throws Exception {
    DataSetManager manager = datasetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                               DataSetAccessor.Namespace.SYSTEM);
    manager.upgrade(META_DATA_TABLE_NAME, new Properties());
  }

  /**
   * TransactionExecutor wraps exceptions into a TransactionFailureException. But the methods in this class
   * only throw OperationException. This method checks whether the wrapped exception is an OperationException
   * and, if so, throws that exception. Otherwise it re-wraps the exception into an Operationexception with
   * status code of internal error, and throws that.
   * @return nothing, because it always throws. However, declaring an exception to be returned alllows callers to
   *         throw that exception, to tell the java compiler that execution is not continuing.
   */
  private OperationException propagateException(TransactionFailureException e) throws OperationException {
    if (e.getCause() != null && e.getCause() instanceof OperationException) {
      throw (OperationException) e.getCause();
    } else {
      // some other problem
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e.getCause());
    }
  }

  //----------- helpers to deal with row/column keys ---------------------

  private static byte[] makeRowKey(String account) {
    return Bytes.toBytes(account);
  }

  private static String extractAccountFromRowKey(byte[] rowKey) {
    return Bytes.toString(rowKey);
  }

  private static byte[] makeColumnKey(String app, String type, String id) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    str.append('\0');
    if (app != null) {
      str.append(app);
    }
    str.append('\0');
    str.append(id);
    return Bytes.toBytes(str.toString());
  }

  byte[] makeColumnKey(MetaDataEntry meta) {
    return makeColumnKey(meta.getApplication(), meta.getType(), meta.getId());
  }

  private static byte[] startColumnKey(String app, String type) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    if (app != null) {
      str.append('\0');
      str.append(app);
    }
    str.append('\0');
    return Bytes.toBytes(str.toString());
  }

  private static byte[] stopColumnKey(String app, String type) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    if (app != null) {
      str.append('\0');
      str.append(app);
    }
    str.append('\1');
    return Bytes.toBytes(str.toString());
  }

  private static String extractApplication(byte[] columnKey) {
    String str = Bytes.toString(columnKey);
    int pos = str.indexOf('\0');
    if (pos < 0) {
      return null;
    }
    int pos1 = str.indexOf('\0', pos + 1);
    if (pos1 < 0) {
      return null;
    }
    if (pos1 == pos + 1) {
      return null;
    }
    return str.substring(pos + 1, pos1);
  }

  /**
   * Helper class for thread local structure.
   */
  private static class PerThread {
    final MetaDataSerializer serializer;
    final OrderedColumnarTable table;
    final TransactionExecutor executor;

    private PerThread(MetaDataSerializer serializer, OrderedColumnarTable table, TransactionExecutor executor) {
      this.serializer = serializer;
      this.table = table;
      this.executor = executor;
    }

    private MetaDataSerializer getSerializer() {
      return serializer;
    }

    private OrderedColumnarTable getTable() {
      return table;
    }

    private TransactionExecutor getExecutor() {
      return executor;
    }
  }

}
