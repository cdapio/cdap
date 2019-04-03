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

package co.cask.cdap.operations;

import com.google.inject.Injector;

import javax.management.MBeanServer;
import javax.management.MXBean;

/**
 * Interface for all operational stats emitted using the operational stats extension framework.
 *
 * To emit stats using this framework, create JMX {@link MXBean} interfaces, then have the implementations of those
 * interfaces extend this class. At runtime, all sub-classes of this class will be registered with the
 * {@link MBeanServer} with the <i>name</i> property determined by {@link #getServiceName()} and the <i>type</i>
 * property determined by {@link #getStatType()}.
 */
public interface OperationalStats {

  /**
   * Initializes the operational stats extension. Called immediately after loading the operational stats extension.
   *
   * @param injector an {@link Injector} to inject the necessary CDAP classes
   */
  void initialize(Injector injector);

  /**
   * Returns the service name for which this operational stat is emitted. Service names are case-insensitive, and will
   * be converted to lower case.
   */
  String getServiceName();

  /**
   * Returns the type of the stat. Stat types are case-insensitive, and will be converted to lower case.
   */
  String getStatType();

  /**
   * Collects the stats that are reported by this object.
   */
  void collect() throws Exception;

  /**
   * Performs any cleanup as necessary.
   */
  void destroy();
}
