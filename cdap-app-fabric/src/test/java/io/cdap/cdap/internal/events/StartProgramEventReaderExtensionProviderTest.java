/*
 * Copyright © 2023 Cask Data, Inc.
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
import io.cdap.cdap.internal.events.dummy.DummyEventReader;
import io.cdap.cdap.spi.events.EventReader;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link StartProgramEventReaderExtensionProvider}
 */
public class StartProgramEventReaderExtensionProviderTest {

  @Test
  public void testEnabledEventReaderFilter() {
    EventReader mockReader = new DummyEventReader();
    String mockModuleDir = "dir";
    CConfiguration cConf = CConfiguration.create();

    StartProgramEventReaderExtensionProvider readerExtensionProvider1
            = new StartProgramEventReaderExtensionProvider(cConf);
    Set<String> test1 = readerExtensionProvider1.getSupportedTypesForProvider(mockReader,
            mockModuleDir);
    Assert.assertTrue(test1.isEmpty());

    //Test with reader dir enabled
    cConf.setStrings(Constants.Event.START_EVENTS_READER_EXTENSIONS_ENABLED_LIST, mockModuleDir);
    StartProgramEventReaderExtensionProvider readerExtensionProvider2
            = new StartProgramEventReaderExtensionProvider(cConf);
    Set<String> test2 = readerExtensionProvider2.getSupportedTypesForProvider(mockReader,
            mockModuleDir);
    Assert.assertTrue(test2.contains(mockModuleDir));
  }
}
