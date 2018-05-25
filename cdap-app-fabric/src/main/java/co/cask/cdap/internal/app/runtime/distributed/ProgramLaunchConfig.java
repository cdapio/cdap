/*
 * Copyright © 2018 Cask Data, Inc.
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

import ch.qos.logback.classic.Level;
import co.cask.cdap.api.Resources;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import com.google.common.collect.Iterables;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillRunnable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Configuration for launching Twill container for a program.
 */
@SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
public final class ProgramLaunchConfig {

  private final Map<String, LocalizeResource> extraResources = new HashMap<>();
  private final List<String> extraClasspath = new ArrayList<>();
  private final Map<String, String> extraEnv = new HashMap<>();
  private final Map<String, RunnableDefinition> runnables = new HashMap<>();
  private final List<Set<String>> launchOrder = new ArrayList<>();
  private final Set<Class<?>> extraDependencies = new HashSet<>();
  private final Map<String, String> extraSystemArguments = new HashMap<>();
  private ClassAcceptor classAcceptor = new HadoopClassExcluder();

  /**
   * Adds extra system arguments that will be available through the {@link ProgramOptions#getArguments()}
   * in the program container.
   */
  public ProgramLaunchConfig addExtraSystemArguments(Map<String, String> args) {
    extraSystemArguments.putAll(args);
    return this;
  }

  public ProgramLaunchConfig addExtraSystemArgument(String key, String value) {
    extraSystemArguments.put(key, value);
    return this;
  }

  public ProgramLaunchConfig addExtraResources(Map<String, LocalizeResource> resources) {
    extraResources.putAll(resources);
    return this;
  }

  public ProgramLaunchConfig addExtraClasspath(Iterable<String> classpath) {
    Iterables.addAll(extraClasspath, classpath);
    return this;
  }

  public ProgramLaunchConfig addExtraEnv(Map<String, String> env) {
    extraEnv.putAll(env);
    return this;
  }

  public ProgramLaunchConfig addRunnable(String name, TwillRunnable runnable, int instances,
                                         Map<String, String> args, Resources defaultResource) {
    return addRunnable(name, runnable, instances, args, defaultResource, null);
  }

  public ProgramLaunchConfig addRunnable(String name, TwillRunnable runnable, int instances,
                                         Map<String, String> args, Resources defaultResources,
                                         @Nullable Integer maxRetries) {
    ResourceSpecification resourceSpec = createResourceSpec(SystemArguments.getResources(args, defaultResources),
                                                            instances);

    Map<String, String> configs = SystemArguments.getTwillContainerConfigs(args, resourceSpec.getMemorySize());
    Map<String, Level> logLevels = SystemArguments.getLogLevels(args);

    runnables.put(name, new RunnableDefinition(runnable, resourceSpec, configs, logLevels, maxRetries));
    return this;
  }

  public ProgramLaunchConfig setLaunchOrder(Iterable<? extends Set<String>> order) {
    launchOrder.clear();
    Iterables.addAll(launchOrder, order);
    return this;
  }

  public ProgramLaunchConfig setClassAcceptor(ClassAcceptor classAcceptor) {
    this.classAcceptor = classAcceptor;
    return this;
  }

  public ProgramLaunchConfig addExtraDependencies(Class<?>...classes) {
    return addExtraDependencies(Arrays.asList(classes));
  }

  public ProgramLaunchConfig addExtraDependencies(Iterable<? extends Class<?>> classes) {
    Iterables.addAll(extraDependencies, classes);
    return this;
  }

  /**
   * Returns the set of extra system arguments.
   */
  public Map<String, String> getExtraSystemArguments() {
    return extraSystemArguments;
  }

  public Map<String, LocalizeResource> getExtraResources() {
    return extraResources;
  }

  public List<String> getExtraClasspath() {
    return extraClasspath;
  }

  public Map<String, String> getExtraEnv() {
    return extraEnv;
  }

  public ClassAcceptor getClassAcceptor() {
    return classAcceptor;
  }

  public Map<String, RunnableDefinition> getRunnables() {
    return runnables;
  }

  public List<Set<String>> getLaunchOrder() {
    return launchOrder;
  }

  public Set<Class<?>> getExtraDependencies() {
    return extraDependencies;
  }

  public ProgramLaunchConfig clearRunnables() {
    runnables.clear();
    return this;
  }

  /**
   * Returns a {@link ResourceSpecification} created from the given {@link Resources} and number of instances.
   */
  private ResourceSpecification createResourceSpec(Resources resources, int instances) {
    return ResourceSpecification.Builder.with()
      .setVirtualCores(resources.getVirtualCores())
      .setMemory(resources.getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }
}
