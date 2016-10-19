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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.proto.id.ProgramId;
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
  public TwillPreparer addLogHandler(LogHandler handler) {
    return delegate.addLogHandler(handler);
  }

  @Override
  @Deprecated
  public TwillPreparer setUser(String user) {
    return delegate.setUser(user);
  }

  @Override
  public TwillPreparer setSchedulerQueue(String name) {
    return delegate.setSchedulerQueue(name);
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    return delegate.setJVMOptions(options);
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    return delegate.addJVMOptions(options);
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return delegate.enableDebugging(runnables);
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    return delegate.enableDebugging(doSuspend, runnables);
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return delegate.withApplicationArguments(args);
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    return delegate.withApplicationArguments(args);
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return delegate.withArguments(runnableName, args);
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    return delegate.withArguments(runnableName, args);
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return delegate.withDependencies(classes);
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    return delegate.withDependencies(classes);
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return delegate.withResources(resources);
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    return delegate.withResources(resources);
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return delegate.withClassPaths(classPaths);
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    return delegate.withClassPaths(classPaths);
  }

  @Override
  public TwillPreparer withEnv(Map<String, String> env) {
    return delegate.withEnv(env);
  }

  @Override
  public TwillPreparer withEnv(String runnableName, Map<String, String> env) {
    return delegate.withEnv(runnableName, env);
  }

  @Override
  public TwillPreparer withApplicationClassPaths(String... classPaths) {
    return delegate.withApplicationClassPaths(classPaths);
  }

  @Override
  public TwillPreparer withApplicationClassPaths(Iterable<String> classPaths) {
    return delegate.withApplicationClassPaths(classPaths);
  }

  @Override
  public TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor) {
    return delegate.withBundlerClassAcceptor(classAcceptor);
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return delegate.addSecureStore(secureStore);
  }

  @Override
  public TwillPreparer setLogLevel(LogEntry.Level logLevel) {
    return delegate.setLogLevel(logLevel);
  }

  @Override
  public TwillController start() {
    try {
      return impersonator.doAs(programId.getNamespaceId(), new Callable<TwillController>() {
        @Override
        public TwillController call() throws Exception {
          return new ImpersonatedTwillController(delegate.start(), impersonator, programId);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
