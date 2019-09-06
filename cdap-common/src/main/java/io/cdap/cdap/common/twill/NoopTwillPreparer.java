/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A no-op implementation of {@link TwillPreparer}.
 */
final class NoopTwillPreparer implements TwillPreparer {
  
  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return this;
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> logLevelsForRunnable) {
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    return this;
  }

  @Override
  public TwillController start() {
    return start(0, TimeUnit.SECONDS);
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    return new NoopTwillController();
  }
}
