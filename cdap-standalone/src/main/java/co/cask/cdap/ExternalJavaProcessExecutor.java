/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * Executor that helps with launching an external Java process.
 */
final class ExternalJavaProcessExecutor extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalJavaProcessExecutor.class);

  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;
  private static final String SEPARATOR = System.getProperty("file.separator");
  private static final String BASE_CLASSPATH = System.getProperty("java.class.path");
  private static final String JAVA_PATH = System.getProperty("java.home") + SEPARATOR + "bin" + SEPARATOR + "java";

  private final String classPath;
  private final String className;
  private final List<String> args;
  private final Map<String, String> processEnv;

  private ExecutorService executor;
  private Process process;
  private Thread shutdownThread;

  ExternalJavaProcessExecutor(String className) {
    this(className, Collections.<String>emptyList());
  }

  ExternalJavaProcessExecutor(String className, List<String> args) {
    this(className, args, Collections.<String, String>emptyMap(), null);
  }

  ExternalJavaProcessExecutor(String className, List<String> args, Map<String, String> processEnv,
                              @Nullable String additionalClassPath) {
    this.classPath = (additionalClassPath == null) ? BASE_CLASSPATH : String.format(
      "%s:%s", additionalClassPath, BASE_CLASSPATH);
    this.className = className;
    this.args = args;
    this.processEnv = Objects.firstNonNull(processEnv, ImmutableMap.<String, String>of());
  }

  @Override
  protected void startUp() throws Exception {
    executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
      String.format("external-java-process-%s", className)).build());
    shutdownThread = createShutdownThread();

    List<String> command = ImmutableList.<String>builder().add(JAVA_PATH, "-cp", classPath, className)
      .addAll(args).build();
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.environment().putAll(processEnv);
    processBuilder.redirectErrorStream(true);
    process = processBuilder.start();
    LOG.info("Process {} started", className);
    executor.execute(createLogRunnable(process));
  }

  @Override
  protected void run() throws Exception {
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      LOG.error("Process {} exited with exit code {}", className, exitCode);
    }
  }

  @Override
  protected void triggerShutdown() {
    executor.shutdownNow();
    shutdownThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    shutdownThread.interrupt();
    shutdownThread.join();
  }

  private Thread createShutdownThread() {
    Thread thread = new Thread(String.format("shutdown-%s", className)) {

      @Override
      public void run() {
        process.destroy();
      }
    };
    thread.setDaemon(true);
    return thread;
  }

  private Runnable createLogRunnable(final Process process) {
    return new Runnable() {
      @Override
      public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                                                                              Charsets.UTF_8))) {
          String line = reader.readLine();
          while (!Thread.currentThread().isInterrupted() && line != null) {
            LOG.info(line);
            line = reader.readLine();
          }
        } catch (IOException e) {
          LOG.error("Exception when reading from stderr stream for {}.", ExternalJavaProcessExecutor.class);
        }
      }
    };
  }
}
