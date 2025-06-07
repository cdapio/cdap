package io.cdap.cdap.storage.spanner;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;

/**
 * An adapter that wraps a Spanner ReadOnlyTransaction to implement the TransactionContext interface.
 * This allows non-locking read-only transactions to be used with CDAP's data access layer.
 * All write methods will throw an UnsupportedOperationException.
 */
public class ReadOnlyTransactionContext implements TransactionContext {
  private final ReadOnlyTransaction delegate;

  ReadOnlyTransactionContext(ReadOnlyTransaction delegate) {
    this.delegate = delegate;
  }

  // --- READ Methods ---
  @Override
  public ResultSet read(String table, KeySet keys, Iterable<String> columns, ReadOption... options) {
    return delegate.read(table, keys, columns, options);
  }

  @Override
  public AsyncResultSet readUsingIndexAsync(String table, String index, KeySet keys, Iterable<String> columns, ReadOption... options) {
    return delegate.readUsingIndexAsync(table, index, keys, columns, options);
  }

  @Override
  public ApiFuture<Struct> readRowUsingIndexAsync(String table, String index, Key key, Iterable<String> columns) {
    return delegate.readRowUsingIndexAsync(table, index, key, columns);
  }


  @Override
  public AsyncResultSet readAsync(String table, KeySet keys, Iterable<String> columns, ReadOption... options) {
    return delegate.readAsync(table, keys, columns, options);
  }

  @Override
  public ApiFuture<Struct> readRowAsync(String table, Key key, Iterable<String> columns) {
    return delegate.readRowAsync(table, key, columns);
  }

  @Override
  public ResultSet readUsingIndex(String table, String index, KeySet keys, Iterable<String> columns, ReadOption... options) {
    return delegate.readUsingIndex(table, index, keys, columns, options);
  }

  @Override
  public Struct readRow(String table, Key key, Iterable<String> columns) {
    return delegate.readRow(table, key, columns);
  }

  @Override
  public Struct readRowUsingIndex(String table, String index, Key key, Iterable<String> columns) {
    return delegate.readRowUsingIndex(table, index, key, columns);
  }

  @Override
  public ResultSet executeQuery(Statement statement, QueryOption... options) {
    return delegate.executeQuery(statement, options);
  }

  @Override
  public AsyncResultSet executeQueryAsync(Statement statement, Options.QueryOption... options) {
    return delegate.executeQueryAsync(statement, options);
  }

  @Override
  public ResultSet analyzeQuery(Statement statement, QueryAnalyzeMode queryAnalyzeMode) {
    return delegate.analyzeQuery(statement, queryAnalyzeMode);
  }

  @Override
  public void close() {
    delegate.close();
  }

  // --- WRITE Methods (All Unsupported) ---
  @Override
  public void buffer(Mutation mutation) {
    throw new UnsupportedOperationException("Write operations are not supported in a read-only transaction.");
  }

  @Override
  public void buffer(Iterable<Mutation> mutations) {
    throw new UnsupportedOperationException("Write operations are not supported in a read-only transaction.");
  }

  @Override
  public ApiFuture<Void> bufferAsync(Mutation mutation) {
    return ApiFutures.immediateFailedFuture(
        new UnsupportedOperationException("Write operations are not supported in a read-only transaction."));
  }

  @Override
  public ApiFuture<Void> bufferAsync(Iterable<Mutation> mutations) {
    return ApiFutures.immediateFailedFuture(
        new UnsupportedOperationException("Write operations are not supported in a read-only transaction."));
  }

  @Override
  public long executeUpdate(Statement statement, Options.UpdateOption... options) {
    throw new UnsupportedOperationException("Write operations are not supported in a read-only transaction.");
  }

  @Override
  public ApiFuture<Long> executeUpdateAsync(Statement statement, Options.UpdateOption... options) {
    return ApiFutures.immediateFailedFuture(
        new UnsupportedOperationException("Write operations are not supported in a read-only transaction."));
  }

  @Override
  public long[] batchUpdate(Iterable<Statement> statements, Options.UpdateOption... options) {
    throw new UnsupportedOperationException("Write operations are not supported in a read-only transaction.");
  }

  @Override
  public ApiFuture<long[]> batchUpdateAsync(Iterable<Statement> statements, Options.UpdateOption... options) {
    return ApiFutures.immediateFailedFuture(
        new UnsupportedOperationException("Write operations are not supported in a read-only transaction."));
  }

  @Override
  public ResultSet analyzeUpdateStatement(Statement statement, QueryAnalyzeMode analyzeMode, Options.UpdateOption... options) {
    throw new UnsupportedOperationException("Write operations are not supported in a read-only transaction.");
  }
}