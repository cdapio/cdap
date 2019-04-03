/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Unit test for {@link CConfigurationUtil#asMap(CConfiguration)} method.
 */
public class CConfigurationMapTest {

  @Test
  public void testGet() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set("test.key", "test.value");

    Assert.assertEquals("test.value", CConfigurationUtil.asMap(cConf).get("test.key"));
  }

  @Test
  public void testEquals() {
    CConfiguration cConf = CConfiguration.create();
    Map<String, String> allEntries = StreamSupport
      .stream(cConf.spliterator(), false)
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Assert.assertEquals(allEntries, CConfigurationUtil.asMap(cConf));
  }

  @Test
  public void testSize() {
    CConfiguration cConf = CConfiguration.create();
    Assert.assertEquals(cConf.size(), CConfigurationUtil.asMap(cConf).size());
  }

  @Test
  public void testIsEmpty() {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();

    Assert.assertTrue(CConfigurationUtil.asMap(cConf).isEmpty());
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testDisallowPut() {
    CConfigurationUtil.asMap(CConfiguration.create()).put("test.key", "test.value");
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testDisallowPutAll() {
    CConfigurationUtil.asMap(CConfiguration.create()).putAll(Collections.singletonMap("test.key", "test.value"));
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testDisallowRemove() {
    CConfigurationUtil.asMap(CConfiguration.create()).remove(Constants.CFG_LOCAL_DATA_DIR);
  }

  @Test (expected = UnsupportedOperationException.class)
  public void testDisallowRemoveSet() {
    CConfigurationUtil.asMap(CConfiguration.create()).entrySet().removeIf(e -> true);
  }
}
