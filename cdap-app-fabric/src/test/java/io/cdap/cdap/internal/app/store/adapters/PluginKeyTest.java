/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.adapters;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PluginKey}.
 */
public class PluginKeyTest {

  @Test
  public void testGettersEqualsHashCodeAndToString() {
    PluginKey key1 = new PluginKey(
        "cdap-data-pipeline", "system", "gcs-connector", "default", "2.1.0", "batchsource", "GCSFile");
    PluginKey key2 = new PluginKey(
        "cdap-data-pipeline", "system", "gcs-connector", "default", "2.1.0", "batchsource", "GCSFile");
    PluginKey keyDifferent = new PluginKey(
        "cdap-data-pipeline", "system", "gcs-connector", "default", "2.2.0", "batchsource", "GCSFile");

    Assert.assertEquals("cdap-data-pipeline", key1.getParentName());
    Assert.assertEquals("system", key1.getParentNamespace());
    Assert.assertEquals("gcs-connector", key1.getArtifactName());
    Assert.assertEquals("default", key1.getArtifactNamespace());
    Assert.assertEquals("2.1.0", key1.getArtifactVersion());
    Assert.assertEquals("batchsource", key1.getPluginType());
    Assert.assertEquals("GCSFile", key1.getPluginName());

    Assert.assertEquals(key1, key2);
    Assert.assertEquals(key1.hashCode(), key2.hashCode());
    Assert.assertNotEquals(key1, keyDifferent);

    String expectedToString = "PluginKey{"
        + "parentName=cdap-data-pipeline, "
        + "parentNamespace=system, "
        + "artifactName=gcs-connector, "
        + "artifactNamespace=default, "
        + "artifactVersion=2.1.0, "
        + "pluginType=batchsource, "
        + "pluginName=GCSFile}";
    Assert.assertEquals(expectedToString, key1.toString());
  }
}
