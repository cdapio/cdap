/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.rpc;

import com.continuuity.common.utils.Networks;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @param <T> The type of service handler interface.
 * @param <I> The type of the thrift service.
 */
public final class ThriftRPCServer<T extends RPCServiceHandler, I> extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftRPCServer.class);

  private final String name;
  private final int ioThreads;
  private final int workerThreads;
  private final int maxReadBufferBytes;
  private final T serviceHandler;
  private final TProcessor processor;

  private InetSocketAddress bindAddress;
  private ExecutorService executor;
  private TServer server;

  /**
   * Creates a {@link Builder} for creating instance of {@link ThriftRPCServer}.
   * @param serviceType Class of the thrift service.
   * @param <I> Type of the thrift service.
   * @return A {@link Builder}.
   */
  public static <I> Builder<I> builder(Class<I> serviceType) {
    return new Builder<I>(serviceType);
  }

  /**
   * Builder for creating instance of ThriftRPCServer. By default, the instance created will bind to
   * random port and with 2 io threads and worker threads equals to min(2, number of cpu cores - 2).
   */
  public static final class Builder<I> {
    private final Class<I> serviceType;
    private String name;
    private InetSocketAddress bindAddress = new InetSocketAddress(0);
    private int ioThreads = 2;
    private int workerThreads = Runtime.getRuntime().availableProcessors() - 2;
    // 16Mb
    private int maxReadBufferBytes = 16 * 1024 * 1024;

    private Builder(Class<I> serviceType) {
      this.serviceType = serviceType;
      this.name = serviceType.getSimpleName();
    }

    public Builder<I> setName(String name) {
      this.name = name;
      return this;
    }

    public Builder<I> setHost(String host) {
      this.bindAddress = new InetSocketAddress(host, bindAddress.getPort());
      return this;
    }

    public Builder<I> setPort(int port) {
      this.bindAddress = new InetSocketAddress(bindAddress.getHostName(), port);
      return this;
    }

    public Builder<I> setIOThreads(int count) {
      this.ioThreads = count;
      return this;
    }

    public Builder<I> setWorkerThreads(int count) {
      this.workerThreads = count;
      return this;
    }

    public Builder<I> setMaxReadBufferBytes(int maxReadBufferBytes) {
      this.maxReadBufferBytes = maxReadBufferBytes;
      return this;
    }

    public <T extends RPCServiceHandler> ThriftRPCServer<T, I> build(T serviceHandler) {
      return new ThriftRPCServer<T, I>(bindAddress, ioThreads, workerThreads, maxReadBufferBytes,
                                       serviceHandler, serviceType, name);
    }
  }

  /**
   * Creates a ThriftRPCServer with the given paramters.
   *
   * @param bindAddress The socket address for the server to listen on. If {@code null}, it'll be binded to random
   *                    port on localhost.
   * @param ioThreads Number of io threads.
   * @param workerThreads Number of worker threads.
   * @param serviceHandler Handler for handling client requests.
   */
  @SuppressWarnings("unchecked")
  private ThriftRPCServer(InetSocketAddress bindAddress, int ioThreads,
                          int workerThreads, int maxReadBufferBytes,
                          T serviceHandler, Class<I> serviceType, String name) {
    Preconditions.checkArgument(ioThreads > 0, "IO threads must be > 0.");
    Preconditions.checkArgument(workerThreads > 0, "Worker threads must be > 0.");

    this.bindAddress = bindAddress;
    this.ioThreads = ioThreads;
    this.workerThreads = workerThreads;
    this.maxReadBufferBytes = maxReadBufferBytes;
    this.serviceHandler = serviceHandler;
    this.name = name;
    this.processor = createProcessor((Class<T>) serviceHandler.getClass(), serviceType);
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  protected void startUp() throws Exception {
    // Determines the address and port to listen on
    InetSocketAddress listenOn = bindAddress;
    if (listenOn == null || listenOn.getPort() <= 0) {
      int port = Networks.getRandomPort();
      if (listenOn == null) {
        listenOn = new InetSocketAddress("localhost", port);
      } else {
        listenOn = new InetSocketAddress(listenOn.getAddress(), port);
      }
    }
    bindAddress = listenOn;

    executor = new ThreadPoolExecutor(0, workerThreads,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(),
                                      Threads.createDaemonThreadFactory(String.format("%s-rpc-%%d", name)),
                                      new ThreadPoolExecutor.CallerRunsPolicy());
    serviceHandler.init();

    TThreadedSelectorServer.Args args =
      new TThreadedSelectorServer.Args(new TNonblockingServerSocket(listenOn))
        .selectorThreads(ioThreads)
        .protocolFactory(new TBinaryProtocol.Factory())
        .transportFactory(new TFramedTransport.Factory())
        .processor(processor)
        .executorService(executor);

    // ENG-443 - Set the max read buffer size. This is important as this will
    // prevent the server from throwing OOME if telnetd to the port
    // it's running on.
    args.maxReadBufferBytes = maxReadBufferBytes;
    server = new TThreadedSelectorServer(args);
    LOG.info("Starting RPC server for {}", name);
  }

  @Override
  protected void shutDown() throws Exception {
    serviceHandler.destroy();
    executor.shutdownNow();
    LOG.info("RPC server for {} stopped.", name);
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Request to stop RPC server for {}", name);
    server.stop();
  }

  @Override
  protected void run() throws Exception {
    server.serve();
  }

  @SuppressWarnings("unchecked")
  private TProcessor createProcessor(final Class<T> handlerType, Class<I> serviceType) {
    // Pick the Iface inner interface and the Processor class
    Class<? extends TProcessor> processorType = null;
    Class<?> ifaceType = null;
    for (Class<?> clz : serviceType.getDeclaredClasses()) {
      if (TProcessor.class.isAssignableFrom(clz)) {
        processorType = (Class<? extends TProcessor>) clz;
      } else if (clz.isInterface() && "Iface".equals(clz.getSimpleName())) {
        ifaceType = clz;
      }
    }

    Preconditions.checkArgument(processorType != null,
                                "Missing TProcessor, %s is not a valid thrift service.", serviceType.getName());
    Preconditions.checkArgument(ifaceType != null,
                                "Missing Iface, %s is not a valid thrift service.", serviceType.getName());

    // If handler already implements the Iface, simply delegate
    if (ifaceType.isAssignableFrom(handlerType)) {
      return createProxyProcessor(handlerType, processorType, ifaceType);
    }

    throw new IllegalArgumentException("Unsupported handler type.");
  }

  private TProcessor createProxyProcessor(final Class<T> handlerType,
                                          Class<? extends TProcessor> processorType, Class<?> ifaceType) {

    try {
      // Map from Iface method to handlerType method to save reflection lookup
      ImmutableMap.Builder<Method, Method> builder = ImmutableMap.builder();
      for (Method method : ifaceType.getMethods()) {
        Method handlerMethod = handlerType.getMethod(method.getName(), method.getParameterTypes());
        if (!handlerMethod.isAccessible()) {
          handlerMethod.setAccessible(true);
        }
        builder.put(method, handlerMethod);
      }
      final Map<Method, Method> methods = builder.build();

      Object proxy = Proxy.newProxyInstance(ifaceType.getClassLoader(),
                                            new Class[]{ifaceType}, new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
          try {
            return methods.get(method).invoke(serviceHandler, args);
          } catch (InvocationTargetException e) {
            if (e.getCause() != null) {
              throw e.getCause();
            } else {
              throw e;
            }
          }
        }
      });

      return processorType.getConstructor(ifaceType).newInstance(proxy);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
