package com.continuuity.internal.app.runtime.batch.inmemory;

import com.google.common.collect.Maps;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.RunIds;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 */
public class ServiceTwillPreparer implements TwillPreparer {

  String extraOptions;
  RunId runId;
  LocationFactory locationFactory;
  TwillSpecification twillSpec;
  Map<String, ExecutorService> runnableExecutorService = Maps.newHashMap();


  public ServiceTwillPreparer(TwillSpecification twillSpec, LocationFactory locationFactory, String extraOptions) {
    this.twillSpec = twillSpec;
    this.locationFactory = locationFactory;
    this.runId = RunIds.generate();
    this.extraOptions = extraOptions;
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    return null;
  }

  @Override
  public TwillPreparer setUser(String user) {
    return null;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
    return null;
  }

  @Override
  public TwillPreparer addJVMOptions(String options) {
    return null;
  }

  @Override
  public TwillPreparer enableDebugging(String... runnables) {
    return null;
  }

  @Override
  public TwillPreparer enableDebugging(boolean doSuspend, String... runnables) {
    return null;
  }

  @Override
  public TwillPreparer withApplicationArguments(String... args) {
    return null;
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    return null;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return null;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    return null;
  }

  @Override
  public TwillPreparer withDependencies(Class<?>... classes) {
    return null;
  }

  @Override
  public TwillPreparer withDependencies(Iterable<Class<?>> classes) {
    return null;
  }

  @Override
  public TwillPreparer withResources(URI... resources) {
    return null;
  }

  @Override
  public TwillPreparer withResources(Iterable<URI> resources) {
    return null;
  }

  @Override
  public TwillPreparer withClassPaths(String... classPaths) {
    return null;
  }

  @Override
  public TwillPreparer withClassPaths(Iterable<String> classPaths) {
    return null;
  }

  @Override
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return null;
  }

  @Override
  public TwillController start() {
    Map<String, RuntimeSpecification> runnables = twillSpec.getRunnables();
    for (Map.Entry<String, RuntimeSpecification> runnable : runnables.entrySet()) {
      String className = runnable.getValue().getRunnableSpecification().getClassName();
      ExecutorService processExecutor = Executors.newSingleThreadExecutor
        (Threads.createDaemonThreadFactory(className + "-executor"));

    }


    return null;
  }
}
