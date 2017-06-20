/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.spark.SparkCredentialsUpdater;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeEnv;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A service that runs in the Spark Driver process in distributed mode. It is responsible for
 * maintaining heartbeat with the client, and optionally transmitting the {@link WorkflowToken}.
 * If runs in secure mode, the service is also responsible for updating the delegation tokens for itself
 * as well as all executors.
 */
public class SparkDriverService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(SparkDriverService.class);
  private static final long HEARTBEAT_INTERVAL_MILLIS = 1000L;
  private static final int MAX_HEARTBEAT_FAILURES = 60;

  private final SparkExecutionClient client;
  @Nullable
  private final SparkCredentialsUpdater credentialsUpdater;
  @Nullable
  private final BasicWorkflowToken workflowToken;

  private Thread runThread;

  public SparkDriverService(URI baseURI, SparkRuntimeContext runtimeContext) {
    this.client = new SparkExecutionClient(baseURI, runtimeContext.getProgramRunId());
    this.credentialsUpdater = createCredentialsUpdater(runtimeContext.getConfiguration(), client);
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    this.workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();

  }

  @Override
  protected void startUp() throws Exception {
    runThread = Thread.currentThread();

    // Make the first heartbeat, fail the startup if failed to make the heartbeat
    heartbeat(client, workflowToken);

    // Schedule the credentials update if necessary
    if (credentialsUpdater != null) {
      credentialsUpdater.startAndWait();
    }

    LOG.info("SparkDriverService started.");
  }

  @Override
  protected void run() throws Exception {
    // Performs heartbeat once per heartbeat interval.
    int failureCount = 0;
    while (isRunning()) {
      try {
        long nextHeartbeatTime = System.currentTimeMillis() + HEARTBEAT_INTERVAL_MILLIS;
        heartbeat(client, workflowToken);
        failureCount = 0;

        long sleepTime = nextHeartbeatTime - System.currentTimeMillis();
        if (isRunning() && sleepTime > 0) {
          TimeUnit.MILLISECONDS.sleep(sleepTime);
        }
      } catch (InterruptedException e) {
        // It's issue on stop. So just continue and let the while loop to handle the condition
        continue;
      } catch (BadRequestException e) {
        LOG.error("Invalid spark program heartbeat. Terminating the execution.", e);
        throw e;
      } catch (Throwable t) {
        if (failureCount++ < MAX_HEARTBEAT_FAILURES) {
          LOG.warn("Failed to make heartbeat for {} times", failureCount, t);
        } else {
          LOG.error("Failed to make heartbeat for {} times. Terminating the execution", failureCount, t);
          throw t;
        }
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Clear the interrupt flag.
    Thread.interrupted();
    try {
      if (credentialsUpdater != null) {
        credentialsUpdater.stopAndWait();
      }
    } finally {
      client.completed(workflowToken);
    }
    LOG.info("SparkDriverService stopped.");
  }

  @Override
  protected void triggerShutdown() {
    if (runThread != null && runThread != Thread.currentThread()) {
      runThread.interrupt();
    }
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, "SparkDriverService");
        thread.setDaemon(true);
        thread.start();
      }
    };
  }

  /**
   * Creates a {@link SparkCredentialsUpdater} for {@link Credentials} in secure environment. If security is disable,
   * or failure to create one due to {@link IOException} from {@link LocationFactory}, {@code null} will be returned.
   */
  @Nullable
  private SparkCredentialsUpdater createCredentialsUpdater(Configuration hConf, SparkExecutionClient client) {
    try {
      SparkConf sparkConf = new SparkConf();
      long updateIntervalMs = sparkConf.getLong("spark.yarn.token.renewal.interval", -1L);
      if (updateIntervalMs <= 0) {
        return null;
      }

      // This env variable is set by Spark for all known Spark versions
      // If it is missing, exception will be thrown
      URI stagingURI = URI.create(System.getenv("SPARK_YARN_STAGING_DIR"));
      LocationFactory lf = new FileContextLocationFactory(hConf);
      Location credentialsDir = stagingURI.isAbsolute()
        ? lf.create(stagingURI.getPath())
        : lf.getHomeLocation().append(stagingURI.getPath());

      LOG.info("Credentials DIR: {}", credentialsDir);

      int daysToKeepFiles = sparkConf.getInt("spark.yarn.credentials.file.retention.days", 5);
      int numFilesToKeep = sparkConf.getInt("spark.yarn.credentials.file.retention.count", 5);
      Location credentialsFile = credentialsDir.append("credentials-" + UUID.randomUUID());

      // Update this property so that the executor will pick it up. It can't get set from the client side,
      // otherwise the AM process will try to look for keytab file
      SparkRuntimeEnv.setProperty("spark.yarn.credentials.file", credentialsFile.toURI().toString());
      return new SparkCredentialsUpdater(createCredentialsSupplier(client, credentialsDir), credentialsDir,
                                         credentialsFile.getName(), updateIntervalMs,
                                         TimeUnit.DAYS.toMillis(daysToKeepFiles), numFilesToKeep);
    } catch (IOException e) {
      LOG.warn("Failed to create credentials updater. Credentials update disabled", e);
      return null;
    }
  }

  /**
   * Creates a {@link Supplier} to supply {@link Credentials} for update. It talks to the {@link SparkExecutionClient}
   * to request for an updated {@link Credentials}.
   */
  private Supplier<Credentials> createCredentialsSupplier(final SparkExecutionClient client,
                                                          final Location credentialsDir) {
    return new Supplier<Credentials>() {
      @Override
      public Credentials get() {
        // Request for the credentials to be written to a temp location
        try {
          Location tmpLocation = credentialsDir.append("fetch-credentials-" + UUID.randomUUID() + ".tmp");
          try {
            client.writeCredentials(tmpLocation);

            // Decode the credentials, update the credentials of the current user and return it
            Credentials credentials = new Credentials();
            try (DataInputStream input = new DataInputStream(tmpLocation.getInputStream())) {
              credentials.readTokenStorageStream(input);
              UserGroupInformation.getCurrentUser().addCredentials(credentials);
              LOG.debug("Credentials updated: {}", credentials.getAllTokens());
              return credentials;
            }
          } finally {
            if (!tmpLocation.delete()) {
              LOG.warn("Failed to delete temporary location {}", tmpLocation);
            }
          }
        } catch (Exception e) {
          // Just throw it out. The SparkCredentialsUpdater will handle it
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Calls the heartbeat endpoint and handle the {@link SparkCommand}.
   * returned from the call.
   */
  private void heartbeat(SparkExecutionClient client, @Nullable BasicWorkflowToken workflowToken) throws Exception {
    LOG.trace("Sending heartbeat with workflow token {}",
              workflowToken == null ? null : workflowToken.getAllFromCurrentNode());
    SparkCommand command = client.heartbeat(workflowToken);
    if (command == null) {
      return;
    }
    if (SparkCommand.STOP.equals(command)) {
      LOG.info("Stop command received from client. Stopping spark program.");
      stop();
    } else {
      LOG.warn("Ignoring unsupported command {}", command);
    }
  }
}
