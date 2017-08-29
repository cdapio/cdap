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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;

import java.io.IOException;

/**
 * This interface abstracts out the details of reading the CConfiguration from HBase using {@link ConfigurationReader}.
 *
 * Implementations differ based on whether it is used inside a coprocessor or client-side, etc.
 */
public interface CConfigurationReader {

  /**
   * Read the configuration.
   */
  CConfiguration read() throws IOException;
}
