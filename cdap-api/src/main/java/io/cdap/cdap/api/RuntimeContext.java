/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.preview.DataTracer;
import org.apache.twill.api.RunId;

import java.util.Map;

/**
 * This interface represents a context for a processor or elements of a processor.
 */
public interface RuntimeContext extends FeatureFlagsProvider {

  /**
   * @return The application specification
   */
  ApplicationSpecification getApplicationSpecification();

  /**
   * @return A map of argument key and value
   */
  Map<String, String> getRuntimeArguments();

  /**
   * @return The cluster name
   */
  String getClusterName();

  /**
   * @return The application namespace
   */
  String getNamespace();

  /**
   * @return The {@link RunId} of the current run
   */
  RunId getRunId();

  /**
   * @return an {@link Admin} to perform admin operations
   */
  Admin getAdmin();

  /**
   * @param dataTracerName the name of the logger using which the debug information will be logged
   *
   * @return an {@link DataTracer} to perform data trace operations.
   */
  @Beta
  DataTracer getDataTracer(String dataTracerName);
}
