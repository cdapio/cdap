package com.continuuity.data.operation.executor.remote;

import java.io.IOException;

/**
 * This interface is used to provide opex service clients to
 * remote operation executor: There is only one (singleton)
 * opex per JVM, but many threads may use it concurrently.
 * However, being a thrift client, it is not thread-safe. In
 * order to avoid serializing all opex calls by synchronizing
 * on the opex service client, we employ a pool of clients. But
 * in different scenarios there are different strategies for
 * pooling: If there are many short-lived threads, it is wise
 * to have a shared pool between all threads. But if there are
 * few long-lived threads, it may be better to have thread-local
 * client for each thread.
 *
 * This interface provides an abstraction of the pooling strategy.
 */
public interface OpexClientProvider {

  /**
   * Initialize the provider. At this point, it should be verified
   * that opex service is up and running and getClient() can
   * create new clients when necessary.
   */
  void initialize() throws IOException;

  /**
   * Retrieve an opex client for exclusive use by the current thread. The
   * opex must be returned to the provider after use.
   * @return an opex client, connected and fully functional
   */
  OperationExecutorClient getClient();

  /**
   * Release an opex client back to the provider's pool.
   * @param client The client to release
   */
  void returnClient(OperationExecutorClient client);

  public <T> T call(Opexable<T> opexable);

  public <T, E extends Exception> T call(Opexeptionable<T, E> opexable)
      throws E;

}
