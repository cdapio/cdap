/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.common.logging.logback.CAppender;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DatasetFramework dsFramework;
  private final CConfiguration conf;

  private ProcedureHandlerMethodFactory handlerMethodFactory;

  private ExecutionHandler executionHandler;
  private ServerBootstrap bootstrap;
  private ChannelGroup channelGroup;
  private BasicProcedureContext procedureContext;

  @Inject
  public ProcedureProgramRunner(DataFabricFacadeFactory dataFabricFacadeFactory, ServiceAnnouncer serviceAnnouncer,
                                @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient,
                                DatasetFramework dsFramework, CConfiguration conf) {
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.dsFramework = dsFramework;
    this.conf = conf;
  }

  @Inject(optional = true)
  void setLogWriter(LogWriter logWriter) {
    CAppender.logWriter = logWriter;
  }

  private BasicProcedureContextFactory createContextFactory(Program program, RunId runId, int instanceId, int count,
                                                            Arguments userArgs, ProcedureSpecification procedureSpec,
                                                            DiscoveryServiceClient discoveryServiceClient) {

    return new BasicProcedureContextFactory(program, runId, instanceId, count, userArgs,
                                            procedureSpec, metricsCollectionService,
                                            discoveryServiceClient, dsFramework, conf);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      ApplicationSpecification appSpec = program.getApplicationSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      ProgramType processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == ProgramType.PROCEDURE, "Only PROCEDURE process type is supported.");

      ProcedureSpecification procedureSpec = appSpec.getProcedures().get(program.getName());
      Preconditions.checkNotNull(procedureSpec, "Missing ProcedureSpecification for %s", program.getName());

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "0"));

      int instanceCount = appSpec.getProcedures().get(program.getName()).getInstances();
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      RunId runId = RunIds.generate();

      BasicProcedureContextFactory contextFactory = createContextFactory(program, runId, instanceId, instanceCount,
                                                                         options.getUserArguments(), procedureSpec,
                                                                         discoveryServiceClient);

      // TODO: A dummy context for getting the cmetrics. We should initialize the dataset here and pass it to
      // HandlerMethodFactory.
      procedureContext = new BasicProcedureContext(program, runId, instanceId, instanceCount,
                                                   ImmutableSet.<String>of(),
                                                   options.getUserArguments(), procedureSpec, metricsCollectionService,
                                                   discoveryServiceClient, dsFramework, conf);

      handlerMethodFactory = new ProcedureHandlerMethodFactory(program, dataFabricFacadeFactory, contextFactory);
      handlerMethodFactory.startAndWait();

      channelGroup = new DefaultChannelGroup();
      executionHandler = createExecutionHandler();
      bootstrap = createBootstrap(program, executionHandler, handlerMethodFactory,
                                  procedureContext.getProgramMetrics(), channelGroup);

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
        .setNameFormat("procedure-worker-" + program.getNamespaceId() + "-%d")
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
                         program.getNamespaceId(), program.getApplicationId(), program.getName());
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
