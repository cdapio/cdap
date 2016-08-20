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
package co.cask.cdap.internal.app.store;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.PreviewId;
import com.google.inject.Injector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link DefaultPreviewStore}.
 */
public class DefaultPreviewStoreTest {
  private static DefaultPreviewStore store;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewStoreTest.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(DefaultPreviewStore.class);
  }

  @Before
  public void before() throws Exception {
    store.clear();
  }

  @Test
  public void testPreviewStore() throws Exception {
    String firstPreview = RunIds.generate().getId();
    PreviewId firstPreviewId = new PreviewId(NamespaceMeta.DEFAULT.getName(), firstPreview);

    String secondPreview = RunIds.generate().toString();
    PreviewId secondPreviewId = new PreviewId(NamespaceMeta.DEFAULT.getName(), secondPreview);

    store.put(firstPreviewId, "mylogger", "key1", "value1");
    store.put(firstPreviewId, "mylogger", "key1", 2);
    store.put(firstPreviewId, "mylogger", "key2", 3);
    store.put(firstPreviewId, "myanotherlogger", "key2", 3);
    Map<Object, Object> propertyMap = new HashMap<>();
    propertyMap.put("key1", "value1");
    propertyMap.put(1, "value2");
    store.put(firstPreviewId, "myanotherlogger", "key2", propertyMap);

    store.put(secondPreviewId, "mylogger", "key1", "value1");
    store.put(secondPreviewId, "myanotherlogger", "key2", 400);

    Map<String, List<String>> firstPreviewData = store.get(firstPreviewId, "mylogger");
    Assert.assertTrue(2 == firstPreviewData.size()); // key1 and key2 are two keys inserted for the firstPreviewId
    Assert.assertEquals(Arrays.asList("\"value1\"", "2"), firstPreviewData.get("key1"));
  }
}
