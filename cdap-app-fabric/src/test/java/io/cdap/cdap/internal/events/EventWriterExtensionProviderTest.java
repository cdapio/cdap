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

package io.cdap.cdap.internal.events;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.events.dummy.DummyEventWriter;
import io.cdap.cdap.spi.events.EventWriter;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link EventWriterExtensionProvider}
 */
public class EventWriterExtensionProviderTest {

  @Test
  public void testEnabledEventWriterFilter() {
    EventWriter mockWriter = new DummyEventWriter();
    String mockWriterName = mockWriter.getID();
    CConfiguration cConf = CConfiguration.create();

    EventWriterExtensionProvider writerExtensionProvider1 = new EventWriterExtensionProvider(cConf);
    Set<String> test1 = writerExtensionProvider1.getSupportedTypesForProvider(mockWriter);
    Assert.assertTrue(test1.isEmpty());

    //Test with writer ID enabled
    cConf.setStrings(Constants.Event.EVENTS_WRITER_EXTENSIONS_ENABLED_LIST, mockWriterName);
    EventWriterExtensionProvider writerExtensionProvider2 = new EventWriterExtensionProvider(cConf);
    Set<String> test2 = writerExtensionProvider2.getSupportedTypesForProvider(mockWriter);
    Assert.assertTrue(test2.contains(mockWriterName));
  }
}
