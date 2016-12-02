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
package co.cask.cdap.internal.app.store.preview;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Injector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link DefaultPreviewStore}.
 */
public class DefaultPreviewStoreTest {

  private static final Gson GSON = new Gson();
  private static DefaultPreviewStore store;

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
    String firstApplication = RunIds.generate().getId();
    ApplicationId firstApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), firstApplication);

    String secondApplication = RunIds.generate().getId();
    ApplicationId secondApplicationId = new ApplicationId(NamespaceMeta.DEFAULT.getName(), secondApplication);

    // put data for the first application
    store.put(firstApplicationId, "mytracer", "key1", "value1");
    store.put(firstApplicationId, "mytracer", "key1", 2);
    store.put(firstApplicationId, "mytracer", "key2", 3);
    Map<Object, Object> propertyMap = new HashMap<>();
    propertyMap.put("key1", "value1");
    propertyMap.put("1", "value2");
    store.put(firstApplicationId, "mytracer", "key2", propertyMap);
    store.put(firstApplicationId, "myanothertracer", "key2", 3);

    // put data for the second application
    store.put(secondApplicationId, "mytracer", "key1", "value1");

    // get the data for first application and logger name "mytracer"
    Map<String, List<JsonElement>> firstApplicationData = store.get(firstApplicationId, "mytracer");
    // key1 and key2 are two keys inserted for the first application.
    Assert.assertEquals(2, firstApplicationData.size());
    Assert.assertEquals("value1", firstApplicationData.get("key1").get(0).getAsString());
    Assert.assertEquals(2, firstApplicationData.get("key1").get(1).getAsInt());
    Assert.assertEquals(3, firstApplicationData.get("key2").get(0).getAsInt());
    Assert.assertEquals(propertyMap, GSON.fromJson(firstApplicationData.get("key2").get(1),
                                                   new TypeToken<HashMap<Object, Object>>() { }.getType()));

    // get the data for second application and logger name "mytracer"
    Map<String, List<JsonElement>> secondApplicationData = store.get(secondApplicationId, "mytracer");
    Assert.assertEquals(1, secondApplicationData.size());
    Assert.assertEquals("value1", secondApplicationData.get("key1").get(0).getAsString());

    // remove the data from first application
    store.remove(firstApplicationId);
    firstApplicationData = store.get(firstApplicationId, "mytracer");
    Assert.assertEquals(0, firstApplicationData.size());
  }
}
