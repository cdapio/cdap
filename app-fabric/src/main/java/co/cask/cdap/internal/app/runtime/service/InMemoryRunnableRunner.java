/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.metrics.ServiceRunnableMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.election.InMemoryElectionRegistry;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.HttpServiceTwillRunnable;
import co.cask.cdap.internal.app.services.ServiceWorkerTwillRunnable;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Runs runnable-service in single-node
 */
public class InMemoryRunnableRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryRunnableRunner.class);

  private final MetricsCollectionService metricsCollectionService;
  private final DiscoveryService dsService;
  private final InMemoryElectionRegistry electionRegistry;
  private final ConcurrentLinkedQueue<Discoverable> discoverables;
  private final TransactionSystemClient transactionSystemClient;
  private final DatasetFramework datasetFramework;
  private final CConfiguration cConfiguration;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public InMemoryRunnableRunner(CConfiguration cConfiguration,
                                DiscoveryServiceClient discoveryServiceClient,
                                DiscoveryService dsService, InMemoryElectionRegistry electionRegistry,
                                MetricsCollectionService metricsCollectionService,
                                TransactionSystemClient transactionSystemClient, DatasetFramework datasetFramework) {
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.dsService = dsService;
    this.electionRegistry = electionRegistry;
    this.discoverables = new ConcurrentLinkedQueue<Discoverable>();
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = datasetFramework;
    this.cConfiguration = cConfiguration;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ProgramController run(final Program program, ProgramOptions options) {
    InMemoryTwillContext twillContext = null;
    try {
      // Extract and verify parameters
      String runnableName = options.getName();
      Preconditions.checkNotNull(runnableName, "Missing runnable name.");

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      String runIdOption = options.getArguments().getOption(ProgramOptionConstants.RUN_ID);
      Preconditions.checkNotNull(runIdOption, "Missing runId");
      RunId runId = RunIds.fromString(runIdOption);

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      ProgramType processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == ProgramType.SERVICE, "Only Service process type is supported.");

      String processorName = program.getName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      ServiceSpecification serviceSpec = appSpec.getServices().get(processorName);
      RuntimeSpecification runnableSpec = serviceSpec.getRunnables().get(runnableName);
      Preconditions.checkNotNull(runnableSpec, "RuntimeSpecification missing for Runnable \"%s\"", runnableName);

      Class<?> clz = null;

      String classStr = runnableSpec.getRunnableSpecification().getClassName();
      // special case for handling http service
      if (classStr.equals(HttpServiceTwillRunnable.class.getName())) {
        clz = HttpServiceTwillRunnable.class;
      } else {
        clz = Class.forName(runnableSpec.getRunnableSpecification().getClassName(),
                            true, program.getClassLoader());
      }

      Preconditions.checkArgument(TwillRunnable.class.isAssignableFrom(clz), "%s is not a TwillRunnable.", clz);

      Class<? extends TwillRunnable> runnableClass = (Class<? extends TwillRunnable>) clz;
      RunId twillRunId = RunIds.generate();
      final String[] argArray = RuntimeArguments.toPosixArray(options.getUserArguments());

      DiscoveryService dService = new DiscoveryService() {
        @Override
        public Cancellable register(final Discoverable discoverable) {
          discoverables.add(discoverable);
          return dsService.register(new Discoverable() {
            @Override
            public String getName() {
              return String.format("service.%s.%s.%s", program.getAccountId(),
                                   program.getApplicationId(), program.getName());
            }

            @Override
            public InetSocketAddress getSocketAddress() {
              return discoverable.getSocketAddress();
            }
          });
        }
      };

      twillContext = new InMemoryTwillContext(twillRunId, runId, InetAddress.getLocalHost(), new String[0], argArray,
                                              runnableSpec.getRunnableSpecification(), instanceId,
                                              runnableSpec.getResourceSpecification().getVirtualCores(),
                                              runnableSpec.getResourceSpecification().getMemorySize(),
                                              discoveryServiceClient, dService, instanceCount, electionRegistry);

      TypeToken<? extends  TwillRunnable> runnableType = TypeToken.of(runnableClass);
      TwillRunnable runnable = null;

      if (runnableClass.isAssignableFrom(HttpServiceTwillRunnable.class)) {
        // Special case for running HTTP services
        runnable = new HttpServiceTwillRunnable(program, runId, cConfiguration, runnableName, metricsCollectionService,
                                                discoveryServiceClient, datasetFramework,
                                                transactionSystemClient);
      } else if (runnableClass.isAssignableFrom(ServiceWorkerTwillRunnable.class)) {
        runnable = new ServiceWorkerTwillRunnable(program, runId, runnableName, program.getClassLoader(),
                                                  cConfiguration, metricsCollectionService, datasetFramework,
                                                  transactionSystemClient, discoveryServiceClient);
      } else {
        runnable = new InstantiatorFactory(false).get(runnableType).create();
      }

      InMemoryRunnableDriver driver = new
        InMemoryRunnableDriver(runnable, twillContext, new UserServiceLoggingContext(program.getAccountId(),
                                                                                     program.getApplicationId(),
                                                                                     processorName,
                                                                                     runnableName));

      //Injecting Metrics
      Reflections.visit(runnable, runnableType,
                        new MetricsFieldSetter(new ServiceRunnableMetrics(metricsCollectionService,
                                                                          program.getApplicationId(),
                                                                          serviceSpec.getName(), runnableName,
                                                                          twillContext.getInstanceId())),
                        new PropertyFieldSetter(runnableSpec.getRunnableSpecification().getConfigs()));

      ProgramController controller = new InMemoryRunnableProgramController(program.getName(), runnableName,
                                                                           twillContext, driver,
                                                                           discoverables);

      LOG.info("Starting Runnable: {}", runnableName);
      driver.start();
      LOG.info("Runnable started: {}", runnableName);

      return controller;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
