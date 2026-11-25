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

package io.cdap.cdap.app.guice;

import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.Element;
import com.google.inject.spi.Elements;
import com.google.inject.spi.ProviderLookup;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

/**
 * Unit tests for {@link AppFabricServiceRuntimeModule}.
 */
public class AppFabricServiceRuntimeModuleTest {

  private static final Key<Supplier<Scheduler>> SCHEDULER_SUPPLIER_KEY =
      Key.get(new TypeLiteral<Supplier<Scheduler>>() { });
  private static final Key<CConfiguration> CCONF_KEY = Key.get(CConfiguration.class);
  private static final Key<DatasetBasedTimeScheduleStore> STORE_KEY =
      Key.get(DatasetBasedTimeScheduleStore.class);

  private Supplier<Scheduler> createSchedulerSupplier(CConfiguration cConf,
      DatasetBasedTimeScheduleStore scheduleStore) {
    Module runtimeModule = new AppFabricServiceRuntimeModule(
        cConf, AppFabricServiceRuntimeModule.ServiceType.SERVER).getInMemoryModules();
    List<Element> elements = Elements.getElements(runtimeModule);
    List<Element> selectedElements = new ArrayList<>();
    boolean foundBinding = false;
    for (Element element : elements) {
      if (element instanceof Binding
          && SCHEDULER_SUPPLIER_KEY.equals(((Binding<?>) element).getKey())) {
        selectedElements.add(element);
        foundBinding = true;
      } else if (element instanceof ProviderLookup) {
        Key<?> lookupKey = ((ProviderLookup<?>) element).getKey();
        if (CCONF_KEY.equals(lookupKey) || STORE_KEY.equals(lookupKey)) {
          selectedElements.add(element);
        }
      }
    }
    Assert.assertTrue("Expected Supplier<Scheduler> binding in AppFabricServiceRuntimeModule",
        foundBinding);

    Injector injector = Guice.createInjector(
        Elements.getModule(selectedElements),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(CConfiguration.class).toInstance(cConf);
            bind(DatasetBasedTimeScheduleStore.class).toInstance(scheduleStore);
          }
        });
    return injector.getInstance(SCHEDULER_SUPPLIER_KEY);
  }

  @Test
  public void testSchedulerSupplierReturnsSingleton() throws SchedulerException {
    CConfiguration cConf = CConfiguration.create();
    DatasetBasedTimeScheduleStore scheduleStore = Mockito.mock(DatasetBasedTimeScheduleStore.class);
    Supplier<Scheduler> supplier = createSchedulerSupplier(cConf, scheduleStore);

    Scheduler first = supplier.get();
    Assert.assertNotNull(first);
    try {
      Scheduler second = supplier.get();
      Assert.assertSame(first, second);
      Mockito.verify(scheduleStore, Mockito.times(1))
          .initialize(Mockito.any(), Mockito.any());
    } finally {
      first.shutdown();
    }
  }

  @Test
  public void testSchedulerSupplierPropagatesRuntimeExceptionUnwrapped() {
    CConfiguration cConf = CConfiguration.create();
    DatasetBasedTimeScheduleStore scheduleStore = Mockito.mock(DatasetBasedTimeScheduleStore.class);
    IllegalStateException expected = new IllegalStateException("store init failure");
    Mockito.doThrow(expected).when(scheduleStore).initialize(Mockito.any(), Mockito.any());

    Supplier<Scheduler> supplier = createSchedulerSupplier(cConf, scheduleStore);
    try {
      supplier.get();
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testSchedulerSupplierWrapsCheckedSchedulerException() {
    CConfiguration cConf = CConfiguration.create();
    DatasetBasedTimeScheduleStore scheduleStore = Mockito.mock(DatasetBasedTimeScheduleStore.class);
    SchedulerException checked = new SchedulerException("checked quartz failure");
    Mockito.doAnswer(invocation -> {
      throw checked;
    }).when(scheduleStore).initialize(Mockito.any(), Mockito.any());

    Supplier<Scheduler> supplier = createSchedulerSupplier(cConf, scheduleStore);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class, supplier::get);
    Assert.assertSame(checked, thrown.getCause());
  }
}
