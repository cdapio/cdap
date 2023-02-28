/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.metadata;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.metadata.dummy.DummyMetadataConsumer;
import io.cdap.cdap.spi.metadata.MetadataConsumer;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MetadataConsumerProvider}
 */
public class MetadataConsumerExtensionProviderTest {

  @Test
  public void testEnabledMetadataConsumerFilter() {
    MetadataConsumer mockConsumer = new DummyMetadataConsumer();
    String mockConsumerName = mockConsumer.getName();
    CConfiguration cConf = CConfiguration.create();

    MetadataConsumerExtensionLoader metadataConsumerExtensionLoader = new MetadataConsumerExtensionLoader(cConf);
    Set<String> test1 = metadataConsumerExtensionLoader.getSupportedTypesForProvider(mockConsumer);
    Assert.assertTrue(test1.isEmpty());

    //Test with metadata consumer name enabled
    cConf.setStrings(Constants.MetadataConsumer.METADATA_CONSUMER_EXTENSIONS_ENABLED_LIST, mockConsumerName);
    MetadataConsumerExtensionLoader metadataConsumerExtensionLoader2 = new MetadataConsumerExtensionLoader(cConf);
    Set<String> test2 = metadataConsumerExtensionLoader2.getSupportedTypesForProvider(mockConsumer);
    Assert.assertTrue(test2.contains(mockConsumerName));
  }
}
