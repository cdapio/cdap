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

import static org.mockito.Matchers.any;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.events.dummy.DummyEventReader;
import io.cdap.cdap.internal.events.dummy.DummyEventReaderExtensionProvider;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.events.StartProgramEvent;
import io.cdap.cdap.spi.events.StartProgramEventDetails;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the {@link StartProgramEventSubscriber}.
 */
public class StartProgramEventSubscriberTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(StartProgramEventSubscriberTest.class);
  private ProgramLifecycleService lifecycleService;
  private CConfiguration cConf;
  private DummyEventReader<StartProgramEvent> eventReader;
  private Injector injector;
  private StartProgramEventSubscriber eventHandler;

  @Before
  public void setup() {
    lifecycleService = Mockito.mock(ProgramLifecycleService.class);
    cConf = Mockito.mock(CConfiguration.class);
    eventReader = new DummyEventReader<>(mockedEvents());
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(ProgramLifecycleService.class).toInstance(lifecycleService);
        bind(CConfiguration.class).toInstance(cConf);
        bind(new TypeLiteral<EventReaderProvider<StartProgramEvent>>() {
        })
            .toInstance(new DummyEventReaderExtensionProvider<StartProgramEvent>(eventReader));
        bind(StartProgramEventSubscriber.class).in(Scopes.SINGLETON);
      }
    });
    eventHandler = injector.getInstance(StartProgramEventSubscriber.class);
    Mockito.doReturn(1).when(cConf).getInt(Constants.Event.START_PROGRAM_EVENT_FETCH_SIZE);
  }

  @Test
  public void testInitialize() {
    eventHandler.initialize();
  }

  @Test
  public void testMessageWorkflow() throws Exception {
    assert (lifecycleService != null);
    Mockito.doReturn(RunIds.generate()).when(lifecycleService).run((ProgramReference) any(), any(),
        Mockito.anyBoolean());

    eventHandler.initialize();
    eventHandler.processEvents(eventReader);
    Mockito.verify(lifecycleService).run((ProgramReference) any(), any(), Mockito.anyBoolean());
  }

  private Collection<StartProgramEvent> mockedEvents() {
    ArrayList<StartProgramEvent> eventList = new ArrayList<>();
    eventList.add(new StartProgramEvent(1, "1",
        new StartProgramEventDetails("app1",
            "namespace1", "id1", "workflows", null)));
    return eventList;
  }
}
