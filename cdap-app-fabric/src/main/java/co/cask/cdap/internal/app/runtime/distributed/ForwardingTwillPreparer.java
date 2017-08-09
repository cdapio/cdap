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

package co.cask.cdap.internal.app.runtime.distributed;

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
 * A {@link TwillPreparer} that forwards all methods to another {@link TwillPreparer}.
 */
public abstract class ForwardingTwillPreparer implements TwillPreparer {

  /**
   * Returns the {@link TwillPreparer} that this class delegates to.
   */
  public abstract TwillPreparer getDelegate();

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    getDelegate().withConfiguration(config);
    return this;
  }

  @Override
  public TwillPreparer withConfiguration(String runnableName, Map<String, String> config) {
    getDelegate().withConfiguration(runnableName, config);
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    getDelegate().addLogHandler(handler);
    return this;
  }

  @Override
  @Deprecated
  public TwillPreparer setUser(String user) {
    getDelegate().setUser(user);
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    getDelegate().setSchedulerQueue(name);
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    getDelegate().setJVMOptions(options);
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String runnableName, String options) {
    getDelegate().setJVMOptions(runnableName, options);
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    getDelegate().addJVMOptions(options);
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    getDelegate().enableDebugging(runnables);
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    getDelegate().enableDebugging(doSuspend, runnables);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    getDelegate().withApplicationArguments(args);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    getDelegate().withApplicationArguments(args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    getDelegate().withArguments(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    getDelegate().withArguments(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>...classes) {
    getDelegate().withDependencies(classes);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    getDelegate().withDependencies(classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    getDelegate().withResources(resources);
    return this;
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    getDelegate().withResources(resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    getDelegate().withClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    getDelegate().withClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    getDelegate().withEnv(env);
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    getDelegate().withEnv(runnableName, env);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    getDelegate().withApplicationClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    getDelegate().withApplicationClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    getDelegate().withBundlerClassAcceptor(classAcceptor);
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    getDelegate().withMaxRetries(runnableName, maxRetries);
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    getDelegate().addSecureStore(secureStore);
    return this;
  }

  @Override
  @Deprecated
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    getDelegate().setLogLevel(logLevel);
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    getDelegate().setLogLevels(logLevels);
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> logLevelsForRunnable) {
    getDelegate().setLogLevels(runnableName, logLevelsForRunnable);
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    getDelegate().setClassLoader(classLoaderClassName);
    return this;
  }

  @Override
  public TwillController start() {
    return getDelegate().start();
  }

  @Override
  public TwillController start(long timeout, TimeUnit timeoutUnit) {
    return getDelegate().start(timeout, timeoutUnit);
  }
}
