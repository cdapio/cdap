/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link Schedulers}.
 */
public class SchedulersTest {

  @Test
  public void testGetScheduleStoreSuccess() throws Exception {
    StructuredTableContext context = Mockito.mock(StructuredTableContext.class);
    Mockito.when(context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE))
        .thenReturn(Mockito.mock(StructuredTable.class));
    Mockito.when(context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_TRIGGER_TABLE))
        .thenReturn(Mockito.mock(StructuredTable.class));

    ProgramScheduleStoreDataset dataset = Schedulers.getScheduleStore(context);
    Assert.assertNotNull(dataset);
  }

  @Test
  public void testGetScheduleStorePropagatesTableNotFoundExceptionUnwrapped() throws Exception {
    StructuredTableContext context = Mockito.mock(StructuredTableContext.class);
    TableNotFoundException missingTable =
        new TableNotFoundException(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE);
    Mockito.when(context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE))
        .thenThrow(missingTable);

    try {
      Schedulers.getScheduleStore(context);
      Assert.fail("Expected TableNotFoundException propagated unwrapped");
    } catch (TableNotFoundException thrown) {
      Assert.assertSame(missingTable, thrown);
    }
  }

  @Test
  public void testGetScheduleStorePropagatesUncheckedExceptionUnwrapped() throws Exception {
    StructuredTableContext context = Mockito.mock(StructuredTableContext.class);
    IllegalStateException expected = new IllegalStateException("context closed");
    Mockito.when(context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE))
        .thenThrow(expected);

    try {
      Schedulers.getScheduleStore(context);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(expected, thrown);
    }
  }
}
