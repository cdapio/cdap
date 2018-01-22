/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.api.metadata.MetadataScope;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests BasicLineageWriter
 */
public class BasicLineageWriterTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  @Test
  public void testWrites() throws Exception {
    Injector injector = getInjector();
    MetadataStore metadataStore = injector.getInstance(MetadataStore.class);
    LineageStore lineageStore = injector.getInstance(LineageStore.class);
    LineageWriter lineageWriter = new BasicLineageWriter(lineageStore);

    // Define entities
    ProgramId program = new ProgramId(NamespaceId.DEFAULT.getNamespace(), "app", ProgramType.FLOW, "flow");
    StreamId stream = new StreamId(NamespaceId.DEFAULT.getNamespace(), "stream");
    ProgramRunId run1 = new ProgramRunId(program.getNamespace(), program.getApplication(), program.getType(),
                                         program.getEntityName(), RunIds.generate(10000).getId());
    ProgramRunId run2 = new ProgramRunId(program.getNamespace(), program.getApplication(), program.getType(),
                                         program.getEntityName(), RunIds.generate(20000).getId());

    // Tag stream
    metadataStore.addTags(MetadataScope.USER, stream, "stag1", "stag2");
    // Write access for run1
    lineageWriter.addAccess(run1, stream, AccessType.READ);
    Assert.assertEquals(ImmutableSet.of(program, stream), lineageStore.getEntitiesForRun(run1));

    // Record time to verify duplicate writes.
    long beforeSecondTag = System.currentTimeMillis();
    // Wait for next millisecond, since access time is stored in milliseconds.
    TimeUnit.MILLISECONDS.sleep(1);

    // Add another tag to stream
    metadataStore.addTags(MetadataScope.USER, stream, "stag3");
    // Write access for run1 again
    lineageWriter.addAccess(run1, stream, AccessType.READ);
    // The write should be no-op, and access time for run1 should not be updated
    Assert.assertTrue(lineageStore.getAccessTimesForRun(run1).get(0) < beforeSecondTag);

    // However, you can write access for another run
    lineageWriter.addAccess(run2, stream, AccessType.READ);
    // Assert new access time is written
    Assert.assertTrue(lineageStore.getAccessTimesForRun(run2).get(0) >= beforeSecondTag);
  }

  private static Injector getInjector() {
    return dsFrameworkUtil.getInjector().createChildInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class)
            .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
            .to(InMemoryDatasetFramework.class);
          bind(MetadataStore.class).to(DefaultMetadataStore.class);
        }
      }
    );
  }
}
