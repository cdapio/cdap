/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.templates.AdapterConfigurer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Default implementation of {@link AdapterConfigurer} for ETL Batch Tests.
 */
//TODO: Remove/Move this class else where until we figure out how to write tests without AdapterConfigurer dependency
public class MockAdapterConfigurer implements AdapterConfigurer {
  private Schedule schedule;
  private int instances;
  private Map<String, String> arguments = Maps.newHashMap();
  private Resources resources;

  @Override
  public void setSchedule(Schedule schedule) {
    this.schedule = schedule;
  }

  @Override
  public void setInstances(int instances) {
    this.instances = instances;
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }

  @Override
  public void addRuntimeArguments(Map<String, String> arguments) {
    this.arguments.putAll(arguments);
  }

  @Override
  public void addRuntimeArgument(String key, String value) {
    this.arguments.put(key, value);
  }

  public Schedule getSchedule() {
    return schedule;
  }

  public int getInstances() {
    return instances;
  }

  public Map<String, String> getArguments() {
    return ImmutableMap.copyOf(arguments);
  }

  public Resources getResources() {
    return resources;
  }
}
