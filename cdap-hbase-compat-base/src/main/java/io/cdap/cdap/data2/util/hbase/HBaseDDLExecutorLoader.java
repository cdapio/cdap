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

package io.cdap.cdap.data2.util.hbase;

import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Provider for {@link HBaseDDLExecutor}.
 */
public class HBaseDDLExecutorLoader extends AbstractExtensionLoader<String, HBaseDDLExecutor> {

  private static final String HBASE_DDL_EXECUTOR_TYPE = "hbaseDDLExecutorType";

  public HBaseDDLExecutorLoader(String hbaseDDLExecutorExtensionDir) {
    super(hbaseDDLExecutorExtensionDir);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(HBaseDDLExecutor hBaseDDLExecutor) {
    return new HashSet<>(Arrays.asList(HBASE_DDL_EXECUTOR_TYPE));
  }
}
