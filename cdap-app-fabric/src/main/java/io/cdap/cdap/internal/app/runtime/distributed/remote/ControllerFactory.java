/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

class ControllerFactory implements TwillControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ControllerFactory.class);

  private final RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService;
  private final ProgramRunId programRunId;
  private final ProgramOptions programOpts;

  ControllerFactory(RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService, ProgramRunId programRunId,
                    ProgramOptions programOpts) {
    this.remoteExecutionTwillRunnerService = remoteExecutionTwillRunnerService;
    this.programRunId = programRunId;
    this.programOpts = programOpts;
  }

  @Override
  public TwillController create(@Nullable Callable<Void> startupTask, long timeout, TimeUnit timeoutUnit) {
    // Make sure we don't run the startup task and create controller if there is already one existed.
    remoteExecutionTwillRunnerService.getControllersLock().lock();
    try {
      RemoteExecutionTwillController controller = remoteExecutionTwillRunnerService.getController(programRunId);
      if (controller != null) {
        return controller;
      }

      CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
      RemoteProcessController processController = createRemoteProcessController(programRunId, programOpts);
      try {
        controller = createController(programRunId, programOpts, processController, startupTaskCompletion);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create controller for " + programRunId, e);
      }

      // Execute the startup task if provided
      if (startupTask != null) {
        ClassLoader startupClassLoader = Optional
          .ofNullable(Thread.currentThread().getContextClassLoader())
          .orElse(getClass().getClassLoader());
        Future<?> startupTaskFuture = remoteExecutionTwillRunnerService.getScheduler().submit(() -> {
          Map<String, String> systemArgs = programOpts.getArguments().asMap();
          LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
          Cancellable restoreContext = LoggingContextAccessor.setLoggingContext(loggingContext);
          ClassLoader oldCl = ClassLoaders.setContextClassLoader(startupClassLoader);
          try {
            startupTaskCompletion.complete(startupTask.call());
          } catch (Throwable t) {
            startupTaskCompletion.completeExceptionally(t);
          } finally {
            ClassLoaders.setContextClassLoader(oldCl);
            restoreContext.cancel();
          }
        });

        // Schedule the timeout check and cancel the startup task if timeout reached.
        // This is a quick task, hence just piggy back on the monitor scheduler to do so.
        remoteExecutionTwillRunnerService.getScheduler().schedule(() -> {
          if (!startupTaskFuture.isDone()) {
            startupTaskCompletion.completeExceptionally(
              new TimeoutException("Starting of program run " + programRunId + " takes longer then "
                                     + timeout + " " + timeoutUnit.name().toLowerCase()));
          }
        }, timeout, timeoutUnit);

        // If the startup task failed, publish failure state and delete the program running state
        startupTaskCompletion.whenComplete((res, throwable) -> {
          if (throwable == null) {
            LOG.debug("Startup task completed for program run {}", programRunId);
          } else {
            LOG.error("Fail to start program run {}", programRunId, throwable);
            // The startup task completion can be failed in multiple scenarios.
            // It can be caused by the startup task failure.
            // It can also be due to cancellation from the controller, or a start up timeout.
            // In either case, always cancel the startup task. If the task is already completed, there is no.
            startupTaskFuture.cancel(true);
            try {
              // Attempt to force kill the remote process. If there is no such process found, it won't throw.
              processController.kill();
            } catch (Exception e) {
              LOG.warn("Force termination of remote process for {} failed", programRunId, e);
            }
            remoteExecutionTwillRunnerService.getProgramStateWriter().error(programRunId, throwable);
          }
        });
      } else {
        // Otherwise, complete the startup task immediately
        startupTaskCompletion.complete(null);
      }

      LOG.debug("Created controller for program run {}", programRunId);
      remoteExecutionTwillRunnerService.addController(programRunId, controller);
      return controller;
    } finally {
      remoteExecutionTwillRunnerService.getControllersLock().unlock();
    }
  }

  /**
   * Creates a new instance of {@link RemoteExecutionTwillController}.
   */
  protected RemoteExecutionTwillController createController(ProgramRunId programRunId, ProgramOptions programOpts,
                                                            RemoteProcessController processController,
                                                            CompletableFuture<Void> startupTaskCompletion)
    throws Exception {
    ExecutionService remoteExecutionService = createRemoteExecutionService(programRunId, programOpts,
                                                                           processController);

    // Create the controller and start the runtime monitor when the startup task completed successfully.
    RemoteExecutionTwillController controller =
      new RemoteExecutionTwillController(remoteExecutionTwillRunnerService.getcConf(),
                                         programRunId,
                                         startupTaskCompletion,
                                         processController,
                                         remoteExecutionTwillRunnerService.getScheduler(), remoteExecutionService);
    startupTaskCompletion.thenAccept(o -> remoteExecutionService.start());

    // On this controller termination, make sure it is removed from the controllers map and have resources released.
    controller.onTerminated(() -> {
      if (remoteExecutionTwillRunnerService.removeController(programRunId, controller)) {
        controller.complete();
      }
    }, remoteExecutionTwillRunnerService.getScheduler());
    return controller;
  }

  private RemoteProcessController createRemoteProcessController(ProgramRunId programRunId,
                                                                ProgramOptions programOpts) {
    RuntimeJobManager jobManager = remoteExecutionTwillRunnerService
      .getProvisioningService().getRuntimeJobManager(programRunId, programOpts).orElse(null);
    // Use RuntimeJobManager to control the remote process if it is supported
    if (jobManager != null) {
      LOG.debug("Creating controller for program run {} with runtime job manager", programRunId);
      return new RuntimeJobRemoteProcessController(
        programRunId,
        () -> remoteExecutionTwillRunnerService.getProvisioningService().getRuntimeJobManager(programRunId, programOpts)
          .orElseThrow(IllegalStateException::new));
    }

    // Otherwise, default to SSH
    RemoteExecutionTwillRunnerService.ClusterKeyInfo clusterKeyInfo =
      new RemoteExecutionTwillRunnerService.ClusterKeyInfo(remoteExecutionTwillRunnerService.getcConf(),
                                                           programOpts,
                                                           remoteExecutionTwillRunnerService.getLocationFactory());
    SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();
    LOG.debug("Creating controller for program run {} with SSH config {}", programRunId, sshConfig);

    return new SSHRemoteProcessController(programRunId, programOpts, sshConfig,
                                          remoteExecutionTwillRunnerService.getProvisioningService());
  }

  protected ExecutionService createRemoteExecutionService(ProgramRunId programRunId,
                                                          ProgramOptions programOpts,
                                                          RemoteProcessController processController)
    throws IOException {
    // If monitor via URL directly, no need to run service socks proxy
    RuntimeMonitorType monitorType = SystemArguments
      .getRuntimeMonitorType(remoteExecutionTwillRunnerService.getcConf(), programOpts);
    if (monitorType == RuntimeMonitorType.URL) {
      LOG.debug("Monitor program run {} with direct url", programRunId);
      return createExecutionService(programRunId, programOpts, processController);
    }

    // SSH monitor. The remote execution service will starts the service proxy
    RemoteExecutionTwillRunnerService.ClusterKeyInfo clusterKeyInfo =
      new RemoteExecutionTwillRunnerService.ClusterKeyInfo(remoteExecutionTwillRunnerService.getcConf(),
                                                           programOpts,
                                                           remoteExecutionTwillRunnerService.getLocationFactory());
    SSHConfig sshConfig = clusterKeyInfo.getSSHConfig();
    RemoteExecutionService remoteExecutionService =
      new SSHRemoteExecutionService(remoteExecutionTwillRunnerService.getcConf(),
                                    programRunId,
                                    sshConfig,
                                    remoteExecutionTwillRunnerService.getServiceSocksProxyPort(),
                                    processController,
                                    remoteExecutionTwillRunnerService.getProgramStateWriter(),
                                    remoteExecutionTwillRunnerService.getScheduler());
    LOG.debug("Monitor program run {} with SSH config {}", programRunId, sshConfig);
    String proxySecret = clusterKeyInfo.getServerProxySecret();
    remoteExecutionService.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        remoteExecutionTwillRunnerService.getServiceSocksProxyAuthenticator().add(proxySecret);
      }

      @Override
      public void terminated(Service.State from) {
        remoteExecutionTwillRunnerService.getServiceSocksProxyAuthenticator().remove(proxySecret);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        remoteExecutionTwillRunnerService.getServiceSocksProxyAuthenticator().remove(proxySecret);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return remoteExecutionService;
  }

  protected ExecutionService createExecutionService(ProgramRunId programRunId,
                                                    ProgramOptions programOpts,
                                                    RemoteProcessController processController) {
    return new RemoteExecutionService(remoteExecutionTwillRunnerService.getcConf(),
                                      programRunId,
                                      remoteExecutionTwillRunnerService.getScheduler(),
                                      processController,
                                      remoteExecutionTwillRunnerService.getProgramStateWriter());
  }

}
