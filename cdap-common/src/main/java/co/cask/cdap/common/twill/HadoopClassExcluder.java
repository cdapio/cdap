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

package co.cask.cdap.common.twill;

import org.apache.twill.api.ClassAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * Exclude hadoop classes
 */
public class HadoopClassExcluder extends ClassAcceptor {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopClassExcluder.class);
  private final Set<URL> added = new HashSet<>();

  @Override
  public boolean accept(String className, URL classUrl, URL classPathUrl) {
    // exclude hadoop but not hbase and hive packages
    if (className.startsWith("org.apache.hadoop.")) {
      if (className.startsWith("org.apache.hadoop.hive.")) {
        if (added.add(classPathUrl)) {
          LOG.error("added classpath url {} because of class {}", classPathUrl, className);
        }
        return true;
      } else if (className.startsWith("org.apache.hadoop.hbase.")) {
        // exclude tracing dependencies of classes that have dependencies on commons-logging implementation classes
        // so that commons-logging jar is not packaged (this is required so that slf4j is used for log collection)
        boolean answer = !(className.startsWith("org.apache.hadoop.hbase.http.log.LogLevel")
          || className.startsWith("org.apache.hadoop.hbase.http.HttpRequestLog"));
        if (answer && added.add(classPathUrl)) {
          LOG.error("added classpath url {} because of class {}", classPathUrl, className);
        }
        return answer;
      } else {
        return false;
      }
    }
    if (added.add(classPathUrl)) {
      LOG.error("added classpath url {} because of class {}", classPathUrl, className);
    }
    return true;
  }
}
