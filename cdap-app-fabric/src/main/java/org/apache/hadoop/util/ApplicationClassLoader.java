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

package org.apache.hadoop.util;

import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * Replaces the implementation in Hadoop to do classloading for MR jobs in the way CDAP needed.
 */
public class ApplicationClassLoader extends MapReduceClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationClassLoader.class);

  @SuppressWarnings("unused")
  public ApplicationClassLoader(URL[] urls, ClassLoader parent, List<String> systemClasses) {
    super();
    // This constructor is retained for MR framework to call
    LOG.info("ApplicationClassLoader intercepted");
  }

  @SuppressWarnings("unused")
  public ApplicationClassLoader(String classpath, ClassLoader parent,
                                List<String> systemClasses) throws MalformedURLException {
    super();
    // This constructor is retained for MR framework to call
    LOG.info("ApplicationClassLoader intercepted");
  }
}
