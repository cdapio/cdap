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

package io.cdap.cdap.data2.dataset2.cache;

import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.dataset2.DynamicDatasetCache;
import io.cdap.cdap.data2.dataset2.SingleThreadDatasetCache;
import java.io.IOException;
import java.util.Map;
import org.apache.tephra.TransactionFailureException;
import org.junit.Test;

public class SingleThreadDatasetCacheTest extends DynamicDatasetCacheTest {

  @Override
  protected DynamicDatasetCache createCache(SystemDatasetInstantiator instantiator,
                                            Map<String, String> arguments,
                                            Map<String, Map<String, String>> staticDatasets) {
    return new SingleThreadDatasetCache(instantiator, txClient, NAMESPACE, arguments, null, staticDatasets);
  }

  @Test
  public void testDatasetCache() throws IOException, DatasetManagementException, TransactionFailureException {
    testDatasetCache(null);
  }
}
