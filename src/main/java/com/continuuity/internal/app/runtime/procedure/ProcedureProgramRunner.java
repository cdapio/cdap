package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.continuuity.base.Cancellable;
import com.continuuity.discovery.Discoverable;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.TransactionAgentSupplierFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public final class ProcedureProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureProgramRunner.class);

  private static final int MAX_IO_THREADS = 5;
  private static final int MAX_HANDLER_THREADS = 20;

  private final TransactionAgentSupplierFactory txAgentSupplierFactory;
  private final DiscoveryService discoveryService;

  private ExecutionHandler executionHandler;
  private ServerBootstrap bootstrap;
  private Channel serverChannel;
  private ChannelGroup channelGroup;

  @Inject
  public ProcedureProgramRunner(TransactionAgentSupplierFactory txAgentSupplierFactory,
                                DiscoveryService discoveryService) {
    this.txAgentSupplierFactory = txAgentSupplierFactory;
    this.discoveryService = discoveryService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getProcessorType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.PROCEDURE, "Only PROCEDURE process type is supported.");

      ProcedureSpecification procedureSpec = appSpec.getProcedures().get(program.getProgramName());
      Preconditions.checkNotNull(procedureSpec, "Missing ProcedureSpecification for %s", program.getProgramName());

      int instanceId = Integer.parseInt(options.getArguments().getOption("instanceId", "0"));

      RunId runId = RunId.generate();

      channelGroup = new DefaultChannelGroup();
      executionHandler = createExecutionHandler();
      bootstrap = createBootstrap(program, executionHandler,
                                  createHandlerMethodFactory(program, runId, instanceId), channelGroup);
      serverChannel = bootstrap.bind(new InetSocketAddress(0));

      channelGroup.add(serverChannel);

      LOG.info(String.format("Procedure server started for %s.%s listening on %s",
                             program.getApplicationId(), program.getProgramName(), serverChannel.getLocalAddress()));

      return new ProcedureProgramController(program, runId,
                                            discoveryService.register(createDiscoverable(program, serverChannel)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private ServerBootstrap createBootstrap(Program program, ExecutionHandler executionHandler,
                                          HandlerMethodFactory handlerMethodFactory, ChannelGroup channelGroup) {
    // Single thread for boss thread
    Executor bossExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("procedure-boss-" + program.getProgramName() + "-%d")
        .build());

    // Worker threads pool
    Executor workerExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("procedure-worker-" + program.getAccountId() + "-%d")
        .build());

    ServerBootstrap bootstrap = new ServerBootstrap(
                                    new NioServerSocketChannelFactory(bossExecutor,
                                                                      workerExecutor, MAX_IO_THREADS));

    bootstrap.setPipelineFactory(new ProcedurePipelineFactory(executionHandler, handlerMethodFactory, channelGroup));

    return bootstrap;
  }


  private HandlerMethodFactory createHandlerMethodFactory(final Program program,
                                                          final RunId runId,
                                                          final int instanceId) {
    return new HandlerMethodFactory() {
      @Override
      public HandlerMethod create() {
        try {
          return new ProcedureHandlerMethod(program, runId, instanceId, txAgentSupplierFactory);
        } catch (ClassNotFoundException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private ExecutionHandler createExecutionHandler() {
    ThreadFactory threadFactory = new ThreadFactory() {
      private final ThreadGroup threadGroup = new ThreadGroup("procedure-thread");
      private final AtomicLong count = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r, String.format("procedure-executor-%d", count.getAndIncrement()));
        t.setDaemon(true);
        return t;
      }
    };

    return new ExecutionHandler(new ThreadPoolExecutor(0, MAX_HANDLER_THREADS,
                                                       60L, TimeUnit.SECONDS,
                                                       new SynchronousQueue<Runnable>(),
                                                       threadFactory, new ThreadPoolExecutor.CallerRunsPolicy()));
  }

  private Discoverable createDiscoverable(Program program, Channel serverChannel) {
    final String name = String.format("procedure.%s.%s.%s",
                                      program.getAccountId(),
                                      program.getApplicationId(),
                                      program.getProgramName());
    final InetSocketAddress address = (InetSocketAddress)serverChannel.getLocalAddress();

    return new Discoverable() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return address;
      }
    };
  }

  private final class ProcedureProgramController extends AbstractProgramController {

    private final Cancellable cancellable;

    ProcedureProgramController(Program program, RunId runId, Cancellable cancellable) {
      super(program.getProgramName(), runId);
      this.cancellable = cancellable;
    }

    @Override
    protected void doSuspend() throws Exception {
      // No-op
    }

    @Override
    protected void doResume() throws Exception {
      // No-op
    }

    @Override
    protected void doStop() throws Exception {
      cancellable.cancel();
      try {
        channelGroup.close().await();
      } finally {
        bootstrap.releaseExternalResources();
        executionHandler.releaseExternalResources();
      }
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      // No-op
    }
  }
}
