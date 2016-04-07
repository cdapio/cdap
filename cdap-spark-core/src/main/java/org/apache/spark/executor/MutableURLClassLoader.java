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

package org.apache.spark.executor;

import java.net.URL;

/**
 * Replaces the interface defined in Spark 1.2. We need this because CDAP has dependency on Spark 1.3, hence doesn't
 * have this interface for the {@link ExecutorURLClassLoader} to implements for Spark 1.2 compatibility.
 */
public interface MutableURLClassLoader {

  /**
   * Add a URL.
   */
  void addURL(URL url);

  /**
   * Gets all URLs from this class loader.
   */
  URL[] getURLs();
}
