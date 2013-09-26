package com.continuuity.data2.transaction.distributed;

import org.apache.thrift.TException;

/**
 * This interface is used to provide thrift tx service clients:
 * there is only one (singleton)
 * tx service per JVM, but many threads may use it concurrently.
 * However, being a thrift client, it is not thread-safe. In
 * order to avoid serializing all tx calls by synchronizing
 * on the tx service client, we employ a pool of clients. But
 * in different scenarios there are different strategies for
 * pooling: If there are many short-lived threads, it is wise
 * to have a shared pool between all threads. But if there are
 * few long-lived threads, it may be better to have thread-local
 * client for each thread.
 *
 * This interface provides an abstraction of the pooling strategy.
 */
public interface ThriftClientProvider {

  /**
   * Initialize the provider. At this point, it should be verified
   * that tx service is up and running and getClient() can
   * create new clients when necessary.
   */
  void initialize() throws TException;

  /**
   * Retrieve an tx client for exclusive use by the current thread. The
   * tx must be returned to the provider after use.
   * @return an tx client, connected and fully functional
   */
  TransactionServiceThriftClient getClient() throws TException;

  /**
   * Release an tx client back to the provider's pool.
   * @param client The client to release
   */
  void returnClient(TransactionServiceThriftClient client);

  /**
   * Discard an tx client from the provider's pool. This is called
   * after a client becomes disfunctional, for instance, due to a socket
   * exception. The provider must make sure to close the client, and it
   * must remove the client from its arsenal and be prepared to create
   * a new client subsequently.
   * @param client The client to discard
   */
  void discardClient(TransactionServiceThriftClient client);
}
