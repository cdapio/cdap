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
package org.apache.twill.internal;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.launcher.FindFreePort;
import org.apache.twill.launcher.TwillLauncher;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class helps launching a container.
 */
public final class TwillContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(TwillContainerLauncher.class);

  private final RuntimeSpecification runtimeSpec;
  private final ContainerInfo containerInfo;
  private final ProcessLauncher.PrepareLaunchContext launchContext;
  private final ZKClient zkClient;
  private final int instanceCount;
  private final JvmOptions jvmOpts;
  private final int reservedMemory;
  private final Location secureStoreLocation;

  public TwillContainerLauncher(RuntimeSpecification runtimeSpec, ContainerInfo containerInfo,
                                ProcessLauncher.PrepareLaunchContext launchContext,
                                ZKClient zkClient, int instanceCount, JvmOptions jvmOpts, int reservedMemory,
                                Location secureStoreLocation) {
    this.runtimeSpec = runtimeSpec;
    this.containerInfo = containerInfo;
    this.launchContext = launchContext;
    this.zkClient = zkClient;
    this.instanceCount = instanceCount;
    this.jvmOpts = jvmOpts;
    this.reservedMemory = reservedMemory;
    this.secureStoreLocation = secureStoreLocation;
  }

  /**
   * Start execution run for a class in a container. Will return instance of {@link TwillContainerController}
   * that allows sending messages directly to the container.
   *
   * @param runId Use to represent unique id of the container run.
   * @param instanceId The Twill instance Id.
   * @param mainClass The main class to run in the container.
   * @param classPath The class path to load classes for the container.
   * @return instance of {@link TwillContainerController} to control the container run.
   */
  public TwillContainerController start(RunId runId, int instanceId, Class<?> mainClass, String classPath) {
    // Clean up zookeeper path in case this is a retry and there are old messages and state there.
    Futures.getUnchecked(ZKOperations.ignoreError(
      ZKOperations.recursiveDelete(zkClient, "/" + runId), KeeperException.NoNodeException.class, null));

    // Adds all file to be localized to container
    launchContext.addResources(runtimeSpec.getLocalFiles());

    // Optionally localize secure store.
    try {
      if (secureStoreLocation != null && secureStoreLocation.exists()) {
        launchContext.addResources(new DefaultLocalFile(Constants.Files.CREDENTIALS,
                                                        secureStoreLocation.toURI(),
                                                        secureStoreLocation.lastModified(),
                                                        secureStoreLocation.length(), false, null));
      }
    } catch (IOException e) {
      LOG.warn("Failed to launch container with secure store {}.", secureStoreLocation);
    }

    // Currently no reporting is supported for runnable containers
    launchContext
      .addEnvironment(EnvKeys.TWILL_RUN_ID, runId.getId())
      .addEnvironment(EnvKeys.TWILL_RUNNABLE_NAME, runtimeSpec.getName())
      .addEnvironment(EnvKeys.TWILL_INSTANCE_ID, Integer.toString(instanceId))
      .addEnvironment(EnvKeys.TWILL_INSTANCE_COUNT, Integer.toString(instanceCount));

    // assemble the command based on jvm options
    ImmutableList.Builder<String> commandBuilder = ImmutableList.builder();
    String firstCommand;
    if (jvmOpts.getDebugOptions().doDebug(runtimeSpec.getName())) {
      // for debugging we run a quick Java program to find a free port, then pass that port as the debug port and also
      // as a System property to the runnable (Java has no general way to determine the port from within the JVM).
      // PORT=$(java FindFreePort) && java -agentlib:jdwp=...,address=\$PORT -Dtwill.debug.port=\$PORT... TwillLauncher
      // The $ must be escaped, otherwise it gets expanded (to "") before the command is submitted.
      String suspend = jvmOpts.getDebugOptions().doSuspend() ? "y" : "n";
      firstCommand = "TWILL_DEBUG_PORT=$($JAVA_HOME/bin/java";
      commandBuilder.add("-cp", Constants.Files.LAUNCHER_JAR,
                         FindFreePort.class.getName() + ")",
                         "&&", // this will stop if FindFreePort fails
                         "$JAVA_HOME/bin/java",
                         "-agentlib:jdwp=transport=dt_socket,server=y,suspend=" + suspend + "," +
                           "address=\\$TWILL_DEBUG_PORT",
                         "-Dtwill.debug.port=\\$TWILL_DEBUG_PORT"
                         );
    } else {
      firstCommand = "$JAVA_HOME/bin/java";
    }

    // Back-porting the feature introduced in TWILL-216
    String heapMinRatioStr = System.getenv().get(co.cask.cdap.common.conf.Constants.ENV_TWILL_HEAP_RESERVED_MIN_RATIO);
    double heapMinRatio = heapMinRatioStr == null ? Constants.HEAP_MIN_RATIO : Double.parseDouble(heapMinRatioStr);

    int memory = Resources.computeMaxHeapSize(containerInfo.getMemoryMB(), reservedMemory, heapMinRatio);
    commandBuilder.add("-Djava.io.tmpdir=tmp",
                       "-Dyarn.container=$" + EnvKeys.YARN_CONTAINER_ID,
                       "-Dtwill.runnable=$" + EnvKeys.TWILL_APP_NAME + ".$" + EnvKeys.TWILL_RUNNABLE_NAME,
                       "-cp", Constants.Files.LAUNCHER_JAR + ":" + classPath,
                       "-Xmx" + memory + "m");
    if (jvmOpts.getExtraOptions() != null) {
      commandBuilder.add(jvmOpts.getExtraOptions());
    }
    commandBuilder.add(TwillLauncher.class.getName(),
                       Constants.Files.CONTAINER_JAR,
                       mainClass.getName(),
                       Boolean.TRUE.toString());
    List<String> command = commandBuilder.build();

    ProcessController<Void> processController = launchContext
      .addCommand(firstCommand, command.toArray(new String[command.size()]))
      .launch();

    TwillContainerControllerImpl controller =
      new TwillContainerControllerImpl(zkClient, runId, runtimeSpec.getName(), instanceId, processController);
    controller.start();
    return controller;
  }

  private static final class TwillContainerControllerImpl extends AbstractZKServiceController
                                                          implements TwillContainerController {

    private final String runnable;
    private final int instanceId;
    private final ProcessController<Void> processController;
    // This latch can be used to wait for container shutdown
    private final CountDownLatch shutdownLatch;
    private volatile ContainerLiveNodeData liveData;

    protected TwillContainerControllerImpl(ZKClient zkClient, RunId runId, String runnable, int instanceId,
                                           ProcessController<Void> processController) {
      super(runId, zkClient);
      this.runnable = runnable;
      this.instanceId = instanceId;
      this.processController = processController;
      this.shutdownLatch = new CountDownLatch(1);
    }

    @Override
    protected void doStartUp() {
      // No-op
    }

    @Override
    protected void doShutDown() {
      // Wait for sometime for the container to stop
      // TODO: Use configurable value for stop time
      int maxWaitSecs = Constants.APPLICATION_MAX_STOP_SECONDS;
      try {
        if (Uninterruptibles.awaitUninterruptibly(shutdownLatch, maxWaitSecs, TimeUnit.SECONDS)) {
          return;
        }
      } catch (Exception e) {
        LOG.error("Got exception while trying to stop runnable {}, instance {}", runnable, instanceId, e);
      }
      // Container has not shutdown even after maxWaitSecs after sending stop message,
      // we'll need to kill the container
      LOG.warn("Killing runnable {}, instance {} after waiting {} secs", runnable, instanceId, maxWaitSecs);
      killAndWait(maxWaitSecs);
    }

    @Override
    protected void instanceNodeUpdated(NodeData nodeData) {
      if (nodeData == null ||  nodeData.getData() == null) {
        LOG.warn("Instance node was updated but data is null.");
        return;
      }
      try {
        Gson gson = new Gson();
        JsonElement json = gson.fromJson(new String(nodeData.getData(), Charsets.UTF_8), JsonElement.class);
        if (json.isJsonObject()) {
          JsonElement data = json.getAsJsonObject().get("data");
          if (data != null) {
            this.liveData = gson.fromJson(data, ContainerLiveNodeData.class);
            LOG.info("Container LiveNodeData updated: " + new String(nodeData.getData(), Charsets.UTF_8));
          }
        }
      } catch (Throwable t) {
        LOG.warn("Error deserializing updated instance node data", t);
      }
    }

    @Override
    protected void instanceNodeFailed(Throwable cause) {
      // No-op
    }

    @Override
    public ListenableFuture<Message> sendMessage(Message message) {
      return sendMessage(message, message);
    }

    @Override
    public void completed(int exitStatus) {
      // count down the shutdownLatch to inform any waiting threads that this container is complete
      shutdownLatch.countDown();
      synchronized (this) {
        forceShutDown();
      }
    }

    @Override
    public ContainerLiveNodeData getLiveNodeData() {
      return liveData;
    }

    @Override
    public void kill() {
      processController.cancel();
    }

    private void killAndWait(int maxWaitSecs) {
      Stopwatch watch = new Stopwatch();
      watch.start();
      int tries = 0;
      while (watch.elapsedTime(TimeUnit.SECONDS) < maxWaitSecs) {
        // Kill the application
        try {
          ++tries;
          kill();
        } catch (Exception e) {
          LOG.error("Exception while killing runnable {}, instance {}", runnable, instanceId, e);
        }

        // Wait on the shutdownLatch,
        // if the runnable has stopped then the latch will be count down by completed() method
        if (Uninterruptibles.awaitUninterruptibly(shutdownLatch, 10, TimeUnit.SECONDS)) {
          // Runnable has stopped now
          return;
        }
      }

      // Timeout reached, runnable has not stopped
      LOG.error("Failed to kill runnable {}, instance {} after {} tries", runnable, instanceId, tries);
      // TODO: should we throw exception here?
    }
  }
}
