package com.continuuity.data.runtime.main;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

/**
 *
 */
public class InMemoryTwillPreparer implements TwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTwillPreparer.class);

  private final TwillSpecification twillSpec;
  private final InMemoryTwillControllerFactory controllerFactory;
  private final RunId runId;
  private String user;
  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();

  InMemoryTwillPreparer(TwillSpecification twillSpec, InMemoryTwillControllerFactory controllerFactory) {
    this.twillSpec = twillSpec;
    this.controllerFactory = controllerFactory;
    this.runId = RunIds.generate();
  }

  @Override
  public TwillPreparer addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    return this;
  }

  @Override
  public TwillPreparer setUser(String user) {
    this.user = user;
    return this;
  }

  @Override
  public TwillPreparer setJVMOptions(String options) {
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
    return withApplicationArguments(ImmutableList.copyOf(args));
  }

  @Override
  public TwillPreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public TwillPreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, ImmutableList.copyOf(args));
  }

  @Override
  public TwillPreparer withArguments(String runnableName, Iterable<String> args) {
    runnableArgs.putAll(runnableName, args);
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
  public TwillPreparer addSecureStore(SecureStore secureStore) {
    return this;
  }

  @Override
  public TwillController start() {
    List<String> orderList = twillSpec.getOrders();
    List<String> runnables = twillSpec.getRunnables();
  }
}
