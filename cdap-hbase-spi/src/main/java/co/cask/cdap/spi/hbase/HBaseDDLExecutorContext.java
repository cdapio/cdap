/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spi.hbase;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;

/**
 * HBase DDL executor context.
 */
@Beta
public interface HBaseDDLExecutorContext {
  /**
   * Returns the hadoop configuration with which the {@link HBaseDDLExecutor} was initialized.
   * @param <T>
   * @return the configuration instance
   */
  <T> T getConfiguration();

  /**
   * Returns the {@link Map} of properties associated with the HBase DDL executor extension.
   * The properties for the extension can be specified in the <code>cdap-site.xml</code>
   * prefixed with <code>cdap.hbase.spi.</code>. The returned {@link Map} will have all
   * such properties, but with prefix <code>cdap.hbase.spi.</code> stripped.
   */
  Map<String, String> getProperties();
}
