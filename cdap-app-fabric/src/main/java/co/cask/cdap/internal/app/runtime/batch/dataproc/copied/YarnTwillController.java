/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc.copied;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.internal.AbstractTwillController;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.appmaster.ApplicationMasterLiveNodeData;
import org.apache.twill.internal.appmaster.TrackerService;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnApplicationReport;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * A {@link org.apache.twill.api.TwillController} that controllers application running on Hadoop YARN.
 */
public final class YarnTwillController extends AbstractTwillController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(YarnTwillController.class);

  private final String appName;
  private final Callable<ProcessController<YarnApplicationReport>> startUp;
  private final long startTimeout;
  private final TimeUnit startTimeoutUnit;
  private volatile ApplicationMasterLiveNodeData amLiveNodeData;
  private ProcessController<YarnApplicationReport> processController;
  private ApplicationAttemptId currentAttemptId;

  // Thread for polling yarn for application status if application got ZK session expire.
  // Only used by the instanceUpdate/Delete method, which is from serialized call from ZK callback.
  private Thread statusPollingThread;

  /**
   * Creates an instance with an existing {@link ApplicationMasterLiveNodeData}.
   */
  public YarnTwillController(String appName, RunId runId, ZKClient zkClient,
                      final ApplicationMasterLiveNodeData amLiveNodeData, final YarnAppClient yarnAppClient) {
    super(appName, runId, zkClient, amLiveNodeData.getKafkaZKConnect() != null, Collections.<LogHandler>emptyList());
    this.appName = appName;
    this.amLiveNodeData = amLiveNodeData;
    this.startUp = new Callable<ProcessController<YarnApplicationReport>>() {
      @Override
      public ProcessController<YarnApplicationReport> call() throws Exception {
        return yarnAppClient.createProcessController(
          YarnUtils.createApplicationId(amLiveNodeData.getAppIdClusterTime(), amLiveNodeData.getAppId()));
      }
    };
    this.startTimeout = Constants.APPLICATION_MAX_START_SECONDS;
    this.startTimeoutUnit = TimeUnit.SECONDS;
  }

  public YarnTwillController(String appName, RunId runId, ZKClient zkClient, boolean logCollectionEnabled,
                      Iterable<LogHandler> logHandlers, Callable<ProcessController<YarnApplicationReport>> startUp,
                      long startTimeout, TimeUnit startTimeoutUnit) {
    super(appName, runId, zkClient, logCollectionEnabled, logHandlers);
    this.appName = appName;
    this.startUp = startUp;
    this.startTimeout = startTimeout;
    this.startTimeoutUnit = startTimeoutUnit;
  }

  /**
   * Sends a message to application to notify the secure store has be updated.
   */
  ListenableFuture<Void> secureStoreUpdated() {
    return sendMessage(SystemMessages.SECURE_STORE_UPDATED, null);
  }

  @Nullable
  ApplicationMasterLiveNodeData getApplicationMasterLiveNodeData() {
    return amLiveNodeData;
  }

  @Override
  protected void doStartUp() {
    super.doStartUp();

    // Submit and poll the status of the yarn application
    try {
      processController = startUp.call();

      YarnApplicationReport report = processController.getReport();
      ApplicationId appId = report.getApplicationId();
      LOG.info("Application {} with id {} submitted", appName, appId);

      YarnApplicationState state = report.getYarnApplicationState();
      Stopwatch stopWatch = new Stopwatch().start();

      LOG.debug("Checking yarn application status for {} {}", appName, appId);
      while (!hasRun(state) && stopWatch.elapsedTime(startTimeoutUnit) < startTimeout) {
        report = processController.getReport();
        state = report.getYarnApplicationState();
        LOG.debug("Yarn application status for {} {}: {}", appName, appId, state);
        TimeUnit.SECONDS.sleep(1);
      }
      LOG.info("Yarn application {} {} is in state {}", appName, appId, state);
      if (state != YarnApplicationState.RUNNING) {
        LOG.info("Yarn application {} {} is not in running state. Shutting down controller.", appName, appId);
        forceShutDown();
      }

      currentAttemptId = report.getCurrentApplicationAttemptId();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected synchronized void doShutDown() {
    if (processController == null) {
      LOG.warn("No process controller for application that is not submitted.");
      return;
    }

    // Stop polling if it is running.
    stopPollStatus();

    // Wait for the stop message being processed
    try {
      Uninterruptibles.getUninterruptibly(getStopMessageFuture(),
                                          Constants.APPLICATION_MAX_STOP_SECONDS, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Failed to wait for stop message being processed.", e);
      // Kill the application through yarn
      kill();
    }

    FinalApplicationStatus finalStatus;
    // Poll application status from yarn
    try (ProcessController<YarnApplicationReport> processController = this.processController) {
      Stopwatch stopWatch = new Stopwatch().start();
      long maxTime = TimeUnit.MILLISECONDS.convert(Constants.APPLICATION_MAX_STOP_SECONDS, TimeUnit.SECONDS);

      YarnApplicationReport report = processController.getReport();
      finalStatus = report.getFinalApplicationStatus();
      ApplicationId appId = report.getApplicationId();
      while (finalStatus == FinalApplicationStatus.UNDEFINED &&
        stopWatch.elapsedTime(TimeUnit.MILLISECONDS) < maxTime) {
        LOG.debug("Yarn application final status for {} {}: {}", appName, appId, finalStatus);
        TimeUnit.SECONDS.sleep(1);
        finalStatus = processController.getReport().getFinalApplicationStatus();
      }

      // Application not finished after max stop time, kill the application
      if (finalStatus == FinalApplicationStatus.UNDEFINED) {
        kill();
        finalStatus = FinalApplicationStatus.KILLED;
      }
    } catch (Exception e) {
      LOG.warn("Exception while waiting for application report: {}", e.getMessage(), e);
      kill();
      finalStatus = FinalApplicationStatus.KILLED;
    }

    super.doShutDown();

    if (finalStatus == FinalApplicationStatus.FAILED) {
      // If we know the app status is failed, throw an exception to make this controller goes into error state.
      // All other final status are not treated as failure as we can't be sure.
      setTerminationStatus(TerminationStatus.FAILED);
      throw new RuntimeException(String.format("Yarn application completed with failure %s, %s.", appName, getRunId()));
    }
    setTerminationStatus(finalStatus == FinalApplicationStatus.SUCCEEDED
                           ? TerminationStatus.SUCCEEDED : TerminationStatus.KILLED);
  }

  @Override
  public void kill() {
    if (processController != null) {
      YarnApplicationReport report = processController.getReport();
      LOG.info("Killing application {} {}", appName, report.getApplicationId());
      processController.cancel();
    } else {
      LOG.warn("No process controller for application that is not submitted.");
    }
  }

  @Override
  protected void instanceNodeUpdated(NodeData nodeData) {
    throw new UnsupportedOperationException();
//    ApplicationMasterLiveNodeData data = ApplicationMasterLiveNodeDecoder.decode(nodeData);
//    if (data != null) {
//      amLiveNodeData = data;
//    }
  }

  @Override
  protected void instanceNodeFailed(Throwable cause) {
    // Resort to polling from Yarn for the application status.
    if (processController == null) {
      LOG.warn("No process controller for application that is not submitted.");
      return;
    }
    YarnApplicationReport report = processController.getReport();

    // It happens if the application has ZK session expire or the node is deleted due to application termination.
    LOG.info("Failed to access application {} {} live node in ZK, resort to polling. Failure reason: {}",
             appName, report.getApplicationId(), cause == null ? "Unknown" : cause.getMessage());

    startPollStatus(report.getApplicationId());
  }

  private synchronized void startPollStatus(ApplicationId appId) {
    if (statusPollingThread == null) {
      statusPollingThread = new Thread(createStatusPollingRunnable(),
                                       String.format("%s-%s-yarn-poller", appName, appId));
      statusPollingThread.setDaemon(true);
      statusPollingThread.start();
    }
  }

  private synchronized void stopPollStatus() {
    if (statusPollingThread != null) {
      statusPollingThread.interrupt();
      statusPollingThread = null;
    }
  }

  private Runnable createStatusPollingRunnable() {
    return new Runnable() {

      @Override
      public void run() {
        YarnApplicationReport report = processController.getReport();
        ApplicationId appId = report.getApplicationId();
        boolean shutdown = false;
        boolean watchInstanceNode = false;

        try {
          LOG.debug("Polling status from Yarn for {} {}.", appName, appId);
          while (!Thread.currentThread().isInterrupted()) {
            if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
              shutdown = true;
              break;
            }
            ApplicationAttemptId attemptId = report.getCurrentApplicationAttemptId();
            if (currentAttemptId.compareTo(attemptId) != 0) {
              LOG.info("Application attempt ID change from {} to {}", currentAttemptId, attemptId);
              currentAttemptId = attemptId;
              resetLogHandler();
            }

            // Make a sync exists call to instance node and re-watch if the node exists
            try {
              // The timeout is arbitrary, as it's just for avoiding block forever
              Stat stat = zkClient.exists(getInstancePath()).get(5, TimeUnit.SECONDS);
              if (stat != null) {
                watchInstanceNode = true;
                break;
              }
            } catch (ExecutionException e) {
              // Ignore the exception, as any exception won't affect the status polling.
              LOG.debug("Failed in exists call on ZK path {}.", getInstancePath(), e);
            } catch (TimeoutException e) {
              LOG.debug("Timeout in exists call on ZK path {}.", getInstancePath(), e);
            }

            TimeUnit.SECONDS.sleep(1);
            report = processController.getReport();
          }
        } catch (InterruptedException e) {
          // OK to ignore.
          LOG.debug("Status polling thread interrupted for application {} {}", appName, appId);
        }

        LOG.debug("Stop polling status from Yarn for {} {}.", appName, appId);

        if (shutdown) {
          LOG.info("Yarn application {} {} completed. Shutting down controller.", appName, appId);
          forceShutDown();
        } else if (watchInstanceNode) {
          LOG.info("Rewatch instance node for {} {} at {}", appName, appId, getInstancePath());
          synchronized (YarnTwillController.this) {
            statusPollingThread = null;
            watchInstanceNode();
          }
        }
      }
    };
  }

  private boolean hasRun(YarnApplicationState state) {
    switch (state) {
      case RUNNING:
      case FINISHED:
      case FAILED:
      case KILLED:
        return true;
    }
    return false;
  }

  @Override
  public ResourceReport getResourceReport() {
    // Only has resource report if the app is running.
    if (state() != State.RUNNING) {
      return null;
    }
    ResourceReportClient resourcesClient = getResourcesClient();
    return (resourcesClient == null) ? null : resourcesClient.get();
  }

  /**
   * Returns the {@link ResourceReportClient} for fetching resource report from the AM.
   * It first consults the RM for the tracking URL and get the resource report from there.
   */
  @Nullable
  private ResourceReportClient getResourcesClient() {
    YarnApplicationReport report = processController.getReport();
    List<URL> urls = new ArrayList<>(2);

    // Try getting the report from the proxy tracking URL as well as the original tracking URL directly
    // This is mainly to workaround for unit-test that the proxy tracking URL doesn't work well with local address.
    for (String url : Arrays.asList(report.getTrackingUrl(), report.getOriginalTrackingUrl())) {
      if (url != null && !url.equals("N/A")) {
        try {
          URL trackingUrl = new URL(url);
          String path = trackingUrl.getPath();
          if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
          }
          urls.add(new URL(trackingUrl.getProtocol(), trackingUrl.getHost(),
                           trackingUrl.getPort(), path + TrackerService.PATH));
        } catch (MalformedURLException e) {
          LOG.debug("Invalid tracking URL {} from YARN application report for {}:{}", url, appName, getRunId());
        }
      }
    }

    if (urls.isEmpty()) {
      return null;
    }

    return new ResourceReportClient(urls);
  }
}
