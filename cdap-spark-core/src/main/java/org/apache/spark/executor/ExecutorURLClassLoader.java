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

import co.cask.cdap.app.runtime.spark.SparkClassLoader;
import co.cask.cdap.common.lang.ClassLoaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * This class is for Spark 1.2 execution. It replaces the one from Spark for doing
 * class loading in a way that CDAP needs.
 */
@SuppressWarnings("unused")
public class ExecutorURLClassLoader extends ClassLoader implements MutableURLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorURLClassLoader.class);

  /**
   * Constructor called from Spark framework.
   */
  public ExecutorURLClassLoader(URL[] urls, ClassLoader parent) {
    super(createParent(parent));
    LOG.info("ExecutorURLClassLoader intercepted");
  }

  @Override
  public void addURL(URL url) {
    // no-op
  }

  @Override
  public URL[] getURLs() {
    // return empty array
    return new URL[0];
  }

  private static ClassLoader createParent(ClassLoader parent) {
    // If SparkClassLoader is already in the ClassLoader hierarchy, simply use the given parent.
    if (ClassLoaders.find(parent, SparkClassLoader.class) != null) {
      return parent;
    }

    // Ignore the given parent and create a SparkClassLoader as the parent
    // This is what needed in Spark distributed mode
    return SparkClassLoader.create();
  }
}

