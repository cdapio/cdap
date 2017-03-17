/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.yarn;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.Configs;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationMasterInfo;
import org.apache.twill.internal.appmaster.ApplicationMasterProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

/**
 * <p>
 * The service implementation of {@link YarnAppClient} for Apache Hadoop 2.1 and beyond.
 *
 * The {@link VersionDetectYarnAppClientFactory} class will decide to return instance of this class for
 * Apache Hadoop 2.1 and beyond.
 * </p>
 */
// TODO: This is a copy from Twill, and can be removed when TWILL-119 is fixed. CDAP-4923 tracks Twill-0.8.0 upgrade.
// TODO: TWILL-175 will also need to be fixed. A fix has been included in this copied class (createYarnClient method)
// After copying, we have removed the addRMToken() method. That functionality is in YarnTokenUtils.
// Additionally, we have updated it to not have a single YarnClient, but instead a collection of YarnClients, one per
// UGI. We need to do this because YarnClient is not designed to be reusable across UGIs. Even if the calling UGI
// changes, the Yarn user still is the original UGI that constructed the YARN client.
@SuppressWarnings({"unused", "WeakerAccess"})
public final class Hadoop21YarnAppClient extends AbstractIdleService implements YarnAppClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop21YarnAppClient.class);
  private final Configuration configuration;

  public Hadoop21YarnAppClient(Configuration configuration) {
    this.configuration = configuration;
  }

  // Creates and starts a yarn client
  private YarnClient createYarnClient() {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(configuration);
    yarnClient.start();
    return yarnClient;
  }

  @Override
  public ProcessLauncher<ApplicationMasterInfo> createLauncher(TwillSpecification twillSpec,
                                                               @Nullable String schedulerQueue) throws Exception {
    YarnClient yarnClient = createYarnClient();
    try {
      // Request for new application
      YarnClientApplication application = yarnClient.createApplication();
      final GetNewApplicationResponse response = application.getNewApplicationResponse();
      final ApplicationId appId = response.getApplicationId();

      // Setup the context for application submission
      final ApplicationSubmissionContext appSubmissionContext = application.getApplicationSubmissionContext();
      appSubmissionContext.setApplicationId(appId);
      appSubmissionContext.setApplicationName(twillSpec.getName());

      if (schedulerQueue != null) {
        appSubmissionContext.setQueue(schedulerQueue);
      }

      // Set the resource requirement for AM
      // This is back-port from TWILL-90. Since we don't have a copy of twill Configs class, the name of the config
      // is defined as a string
      int memoryMB = configuration.getInt(co.cask.cdap.common.conf.Constants.CFG_TWILL_YARN_AM_MEMORY_MB,
                                          co.cask.cdap.common.conf.Constants.DEFAULT_TWILL_YARN_AM_MEMORY_MB);
      final Resource capability = adjustMemory(response, Resource.newInstance(memoryMB, 1));
      ApplicationMasterInfo appMasterInfo = new ApplicationMasterInfo(appId, capability.getMemory(),
                                                                      capability.getVirtualCores());

      ApplicationSubmitter submitter = new ApplicationSubmitter() {
        @Override
        public ProcessController<YarnApplicationReport> submit(YarnLaunchContext context) {
          ContainerLaunchContext launchContext = context.getLaunchContext();

          appSubmissionContext.setAMContainerSpec(launchContext);
          appSubmissionContext.setResource(capability);
          appSubmissionContext.setMaxAppAttempts(2);

          YarnClient yarnClient = createYarnClient();
          try {
            yarnClient.submitApplication(appSubmissionContext);
            return new ProcessControllerImpl(appId);
          } catch (Exception e) {
            LOG.error("Failed to submit application {}", appId, e);
            throw Throwables.propagate(e);
          } finally {
            yarnClient.stop();
          }
        }
      };

      return new ApplicationMasterProcessLauncher(appMasterInfo, submitter);
    } finally {
      yarnClient.stop();
    }
  }

  private Resource adjustMemory(GetNewApplicationResponse response, Resource capability) {
    int maxMemory = response.getMaximumResourceCapability().getMemory();
    int updatedMemory = capability.getMemory();

    if (updatedMemory > maxMemory) {
      capability.setMemory(maxMemory);
    }

    return capability;
  }

  @Override
  public ProcessLauncher<ApplicationMasterInfo> createLauncher(String user,
                                                               TwillSpecification twillSpec,
                                                               @Nullable String schedulerQueue) throws Exception {
    // Ignore user
    return createLauncher(twillSpec, schedulerQueue);
  }

  @Override
  public ProcessController<YarnApplicationReport> createProcessController(ApplicationId appId) {
    return new ProcessControllerImpl(appId);
  }

  @Override
  public List<NodeReport> getNodeReports() throws Exception {
    YarnClient yarnClient = createYarnClient();
    try {
      return yarnClient.getNodeReports();
    } finally {
      yarnClient.stop();
    }
  }

  @Override
  protected void startUp() throws Exception {
    // no-op. We will create and start YarnClients, on demand
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op. code that calls createYarnClient() is responsible for stopping each individual yarn client
  }

  private final class ProcessControllerImpl implements ProcessController<YarnApplicationReport> {
    private final ApplicationId appId;

    ProcessControllerImpl(ApplicationId appId) {
      this.appId = appId;
    }

    @Override
    public YarnApplicationReport getReport() {
      YarnClient yarnClient = createYarnClient();
      try {
        return new Hadoop21YarnApplicationReport(yarnClient.getApplicationReport(appId));
      } catch (Exception e) {
        LOG.error("Failed to get application report {}", appId, e);
        throw Throwables.propagate(e);
      } finally {
        yarnClient.stop();
      }
    }

    @Override
    public void cancel() {
      YarnClient yarnClient = createYarnClient();
      try {
        yarnClient.killApplication(appId);
      } catch (Exception e) {
        LOG.error("Failed to kill application {}", appId, e);
        throw Throwables.propagate(e);
      } finally {
        yarnClient.stop();
      }
    }
  }
}
