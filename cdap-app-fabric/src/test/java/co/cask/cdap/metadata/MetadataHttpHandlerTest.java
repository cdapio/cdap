/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test helper methods in {@link MetadataHttpHandler}. For handler test please see MetadataHttpHandlerTestRun
 */
public class MetadataHttpHandlerTest {

  @Test
  public void testMakeBackwardCompatible() {
    MetadataEntity.Builder entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.APPLICATION, "app")
      .append(MetadataEntity.VERSION, "ver");

    MetadataEntity.Builder actual = MetadataHttpHandler.makeBackwardCompatible(entity);
    MetadataEntity expected = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.APPLICATION, "app")
      .append(MetadataEntity.VERSION, "ver").build();
    Assert.assertEquals(expected, actual.build());

    entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.ARTIFACT, "art")
      .append(MetadataEntity.VERSION, "ver");

    actual = MetadataHttpHandler.makeBackwardCompatible(entity);
    expected = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.ARTIFACT, "art")
      .append(MetadataEntity.VERSION, "ver").build();
    Assert.assertEquals(expected, actual.build());

    // apps can have no version information
    entity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.APPLICATION, "app");
    actual = MetadataHttpHandler.makeBackwardCompatible(entity);
    expected = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.APPLICATION, "app").build();
    Assert.assertEquals(expected, actual.build());
  }
}
