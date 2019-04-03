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

package co.cask.cdap.proto;

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link NamespaceConfig}
 */
public class NamespaceConfigTest {

  private static final Gson GSON = new Gson();

  /**
   * Test {@link NamespaceConfig} deserialization
   */
  @Test
  public void testNamespaceConfigDeserialize() throws Exception {

    // test that if explore.as.principal is not provided then the default value does get set to true
    String namespaceConfigWithoutExplore = "{\"scheduler.queue.name\":\"someQueue\",\"root.directory\":\"someRoot\"," +
      "\"hbase.namespace\":\"someHbase\",\"hive.database\":\"someHive\"}";
    NamespaceConfig namespaceConfig = GSON.fromJson(namespaceConfigWithoutExplore, NamespaceConfig.class);
    Assert.assertTrue(namespaceConfig.isExploreAsPrincipal());

    // test that if explore.as.principal is provided as false its retained
    String namespaceConfigWithExplore = "{\"scheduler.queue.name\":\"somequeue\",\"root.directory\":\"some\"," +
      "\"explore.as.principal\":false}";
    namespaceConfig = GSON.fromJson(namespaceConfigWithExplore, NamespaceConfig.class);
    Assert.assertFalse(namespaceConfig.isExploreAsPrincipal());
  }
}
