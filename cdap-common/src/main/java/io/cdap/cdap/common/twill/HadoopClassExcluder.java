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

package io.cdap.cdap.common.twill;

import java.net.URL;
import org.apache.twill.api.ClassAcceptor;

/**
 * Exclude hadoop classes.
 */
public class HadoopClassExcluder extends ClassAcceptor {

  @Override
  public boolean accept(String className, URL classUrl, URL classPathUrl) {
    // exclude hadoop but not hbase and hive packages
    if (className.startsWith("org.apache.hadoop.")) {
      if (className.startsWith("org.apache.hadoop.hive.")) {
        return true;
      } else if (className.startsWith("org.apache.hadoop.hbase.")) {
        // exclude tracing dependencies of classes that have dependencies on commons-logging implementation classes
        // so that commons-logging jar is not packaged (this is required so that slf4j is used for log collection)
        return !(className.startsWith("org.apache.hadoop.hbase.http.log.LogLevel")
            || className.startsWith("org.apache.hadoop.hbase.http.HttpRequestLog"));
      } else {
        return false;
      }
    }
    
    // We don't use the log4j-api library from org.apache.logging.
    // However, when ran in distributed mode which contains it,
    // causes conflicts with it if we include it.
    if (className.startsWith("org.apache.logging.log4j")) {
      return false;
    }

    // After Hadoop upgrade to 3.3.6 , It has direct dependency on jackson-databind, which cause conflicts when we pick
    // Hadoop from the runtime environemnt
    // Removing classes related to `com.fasterxml.jackson.core:jackson-databind`
    // and `com.fasterxml.jackson.core:jackson-core`
    if (className.startsWith("com.fasterxml.jackson.databind.")
      || className.startsWith("com.fasterxml.jackson.core.")) {
      return false;
    }

    // We don't use the snappy library from org.iq80. We use the one from org.xerial.snappy.
    // This is an optional dependency from org.iq80.leveldb, hence it is not included in CDAP
    // However, the hive-exec contains it, which can mess up other dependency if we include it.
    return !className.startsWith("org.iq80.snappy.");
  }
}
