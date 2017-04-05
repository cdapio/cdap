/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TwillPreparer} wrapper that provides impersonation support.
 */
final class ImpersonatedTwillPreparer implements TwillPreparer {

  private final TwillPreparer delegate;
  private final Impersonator impersonator;
  private final ProgramId programId;

  ImpersonatedTwillPreparer(TwillPreparer delegate, Impersonator impersonator, ProgramId programId) {
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.programId = programId;
  }

  @Override
  public TwillPreparer withConfiguration(Map<String, String> config) {
    delegate.withConfiguration(config);
    return this;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    delegate.addLogHandler(handler);
    return this;
  }

  @Override
  @Deprecated
  public TwillPreparer setUser(String user) {
    delegate.setUser(user);
    return this;
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    delegate.setSchedulerQueue(name);
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    delegate.setJVMOptions(options);
    return this;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    delegate.addJVMOptions(options);
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    delegate.enableDebugging(runnables);
    return this;
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    delegate.enableDebugging(doSuspend, runnables);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    delegate.withApplicationArguments(args);
    return this;
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    delegate.withApplicationArguments(args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    delegate.withArguments(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    delegate.withArguments(runnableName, args);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    delegate.withDependencies(classes);
    return this;
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    delegate.withDependencies(classes);
    return this;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    delegate.withResources(resources);
    return this;
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    delegate.withResources(resources);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    delegate.withClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    delegate.withClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    delegate.withEnv(env);
    return this;
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    delegate.withEnv(runnableName, env);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    delegate.withApplicationClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    delegate.withApplicationClassPaths(classPaths);
    return this;
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    delegate.withBundlerClassAcceptor(classAcceptor);
    return this;
  }

  @Override
  public TwillPreparer withMaxRetries(String runnableName, int maxRetries) {
    delegate.withMaxRetries(runnableName, maxRetries);
    return this;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    delegate.addSecureStore(secureStore);
    return this;
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    delegate.setLogLevel(logLevel);
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels) {
    delegate.setLogLevels(logLevels);
    return this;
  }

  @Override
  public TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> logLevelsForRunnable) {
    delegate.setLogLevels(runnableName, logLevelsForRunnable);
    return this;
  }

  @Override
  public TwillPreparer setClassLoader(String classLoaderClassName) {
    delegate.setClassLoader(classLoaderClassName);
    return this;
  }

  @Override
  public TwillController start() {
    try {
      return impersonator.doAs(programId, new Callable<TwillController>() {
        @Override
        public TwillController call() throws Exception {
          return new ImpersonatedTwillController(delegate.start(), impersonator, programId);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public TwillController start(final long timeout, final TimeUnit timeoutUnit) {
    try {
      return impersonator.doAs(programId, new Callable<TwillController>() {
        @Override
        public TwillController call() throws Exception {
          return new ImpersonatedTwillController(delegate.start(timeout, timeoutUnit), impersonator, programId);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
