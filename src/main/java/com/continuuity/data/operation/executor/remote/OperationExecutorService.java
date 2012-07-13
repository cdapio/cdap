package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.Increment;
import com.continuuity.api.data.ReadKey;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OperationExecutorService {

  THsHaServer server;
  int port;
  OperationExecutor opex;
  ExecutorService executorService;

  public OperationExecutorService(OperationExecutor opex, int port) {
    this.port = port;
    this.opex = opex;
  }

  public void start() throws TTransportException {
    this.executorService = Executors.newCachedThreadPool();
    THsHaServer.Args serverArgs =
        new THsHaServer.Args(new TNonblockingServerSocket(port))
            .executorService(executorService)
            .processor(new TOperationExecutor.Processor(
                new TOperationExecutorImpl(this.opex)))
            .workerThreads(20);
    this.server = new THsHaServer(serverArgs);
    this.server.serve();
  }

  public static void main(String[] args) throws Exception {
    final int port = Integer.valueOf(args[0]);

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getSingleNodeModules());

    // Get our fully wired Gateway
    final OperationExecutor executor =
        injector.getInstance(OperationExecutor.class);

    executor.execute(new Increment("count".getBytes(), 3));
    System.out.println("First Increment done");
    executor.execute(new Increment("count".getBytes(), 4));
    System.out.println("Second Increment done");
    byte[] bytes = executor.execute(new ReadKey("count".getBytes()));
    if (bytes != null) {
      long value = ByteBuffer.wrap(bytes).asLongBuffer().get();
      System.out.println("value is now: " + value);
    } else {
      System.out.println("value is null");
    }

    new Thread() {
      public void run() {
        OperationExecutorService svc = new OperationExecutorService(executor, port);
        try {
          svc.start();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    RemoteOperationExecutor remote = new RemoteOperationExecutor("localhost", port);

    Increment inc = new Increment("count".getBytes(), 3);
    System.out.println("Submit: " + inc);
    remote.execute(inc);
    bytes = remote.execute(new ReadKey("count".getBytes()));
    if (bytes != null) {
      long value = ByteBuffer.wrap(bytes).asLongBuffer().get();
      System.out.println("value is now: " + value);
    } else {
      System.out.println("value is null");
    }
    bytes = remote.execute(new ReadKey("court".getBytes()));
    if (bytes != null) {
      long value = ByteBuffer.wrap(bytes).asLongBuffer().get();
      System.out.println("value is now: " + value);
    } else {
      System.out.println("value is null");
    }

  }
}
