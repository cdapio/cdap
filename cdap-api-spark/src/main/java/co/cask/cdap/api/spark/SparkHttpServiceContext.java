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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.service.http.HttpServiceContext;

/**
 * A {@link HttpServiceContext} that provides access to {@link org.apache.spark.SparkContext}.
 */
public interface SparkHttpServiceContext extends HttpServiceContext {

  /**
   * @return the {@link org.apache.spark.SparkContext} that is used in the current runtime context
   */
  org.apache.spark.SparkContext getSparkContext();
}
