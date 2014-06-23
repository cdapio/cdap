package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.RunIds;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
  private static final int MAX_HANDLER_THREADS = 100;
  private static final int CLOSE_CHANNEL_TIMEOUT = 5;

  private final DataFabricFacadeFactory dataFabricFacadeFactory;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramServiceDiscovery serviceDiscovery;

  private ProcedureHandlerMethodFactory handlerMethodFactory;

  private ExecutionHandler executionHandler;
  private ServerBootstrap bootstrap;
  private ChannelGroup channelGroup;
  private BasicProcedureContext procedureContext;

  @Inject
  public ProcedureProgramRunner(DataFabricFacadeFactory dataFabricFacadeFactory, ServiceAnnouncer serviceAnnouncer,
                                @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                                MetricsCollectionService metricsCollectionService,
                                ProgramServiceDiscovery serviceDiscovery) {
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
    this.serviceDiscovery = serviceDiscovery;
  }

  @Inject(optional = true)
  void setLogWriter(LogWriter logWriter) {
    CAppender.logWriter = logWriter;
  }

  private BasicProcedureContextFactory createContextFactory(Program program, RunId runId, int instanceId, int count,
                                                            Arguments userArgs, ProcedureSpecification procedureSpec,
                                                            ProgramServiceDiscovery serviceDiscovery) {

    return new BasicProcedureContextFactory(program, runId, instanceId, count, userArgs,
                                            procedureSpec, metricsCollectionService, serviceDiscovery);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.PROCEDURE, "Only PROCEDURE process type is supported.");

      ProcedureSpecification procedureSpec = appSpec.getProcedures().get(program.getName());
      Preconditions.checkNotNull(procedureSpec, "Missing ProcedureSpecification for %s", program.getName());

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "0"));

      int instanceCount = appSpec.getProcedures().get(program.getName()).getInstances();
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      RunId runId = RunIds.generate();

      BasicProcedureContextFactory contextFactory = createContextFactory(program, runId, instanceId, instanceCount,
                                                                         options.getUserArguments(), procedureSpec,
                                                                         serviceDiscovery);

      // TODO: A dummy context for getting the cmetrics. We should initialize the dataset here and pass it to
      // HandlerMethodFactory.
      procedureContext = new BasicProcedureContext(program, runId, instanceId, instanceCount,
                                                   ImmutableMap.<String, Closeable>of(),
                                                   options.getUserArguments(), procedureSpec, metricsCollectionService,
                                                   serviceDiscovery);

      handlerMethodFactory = new ProcedureHandlerMethodFactory(program, dataFabricFacadeFactory, contextFactory);
      handlerMethodFactory.startAndWait();

      channelGroup = new DefaultChannelGroup();
      executionHandler = createExecutionHandler();
      bootstrap = createBootstrap(program, executionHandler, handlerMethodFactory,
                                  procedureContext.getSystemMetrics(), channelGroup);

      // TODO: Might need better way to get the host name
      Channel serverChannel = bootstrap.bind(new InetSocketAddress(hostname, 0));

      channelGroup.add(serverChannel);

      LOG.info(String.format("Procedure server started for %s.%s listening on %s",
                             program.getApplicationId(), program.getName(), serverChannel.getLocalAddress()));

      int servicePort = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
      return new ProcedureProgramController(program, runId,
                                            serviceAnnouncer.announce(getServiceName(program), servicePort));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private ServerBootstrap createBootstrap(Program program, ExecutionHandler executionHandler,
                                          HandlerMethodFactory handlerMethodFactory,
                                          MetricsCollector metrics,
                                          ChannelGroup channelGroup) {
    // Single thread for boss thread
    Executor bossExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("procedure-boss-" + program.getName() + "-%d")
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

    bootstrap.setPipelineFactory(new ProcedurePipelineFactory(executionHandler, handlerMethodFactory,
                                                              metrics, channelGroup));

    return bootstrap;
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

    // Thread pool of max size = MAX_HANDLER_THREADS and will reject new tasks by throwing exceptions
    // The pipeline should have handler to catch the exception and response with status 503.
    ThreadPoolExecutor threadPoolExecutor =
      new OrderedMemoryAwareThreadPoolExecutor(MAX_HANDLER_THREADS, 0, 0, 60L, TimeUnit.SECONDS,
                                               threadFactory);
    threadPoolExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
    return new ExecutionHandler(threadPoolExecutor);
  }

  private String getServiceName(Program program) {
    return String.format("procedure.%s.%s.%s",
                         program.getAccountId(), program.getApplicationId(), program.getName());
  }

  private final class ProcedureProgramController extends AbstractProgramController {

    private final Cancellable cancellable;

    ProcedureProgramController(Program program, RunId runId, Cancellable cancellable) {
      super(program.getName(), runId);
      this.cancellable = cancellable;
      started();
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
      LOG.info("Stopping procedure: " + procedureContext);
      cancellable.cancel();
      try {
        if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS)) {
          LOG.warn("Timeout when closing all channels.");
        }
      } finally {
        bootstrap.releaseExternalResources();
        executionHandler.releaseExternalResources();
      }
      handlerMethodFactory.stopAndWait();

      LOG.info("Procedure stopped: " + procedureContext);
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      // Changing instances in single node is not supported.
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }
      LOG.info("Setting procedure instance in procedure program runner.");
      procedureContext.setInstanceCount((Integer) value);
   }
  }
}
