/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.Reconfigurable;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import io.cdap.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDefinition;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SystemDatasetDefinitionTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static Injector injector;
  private DatasetDefinitionRegistry registry;

  @BeforeClass
  public static void createInjector() {
    injector = Guice.createInjector(
      new ConfigModule(CConfiguration.create()),
      new NonCustomLocationUnitTestModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules());
  }

  @Before
  public void before() throws Exception {
    registry = injector.getInstance(DatasetDefinitionRegistryWithDefaultModules.class);
  }

  // tests that CompositeDatasetDefinition correctly delegates reconfigure() to its embedded types
  @Test
  public void testCompositeDatasetReconfigure() throws IncompatibleUpdateException {
    CompositeDatasetDefinition composite = new CompositeDatasetDefinition(
      "composite", "pedantic", new PedanticDatasetDefinition("pedantic")) {
      @Override
      public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                Map arguments, ClassLoader classLoader) {
        return null;
      }
    };
    DatasetSpecification spec = composite.configure("nn", DatasetProperties.EMPTY);
    DatasetSpecification respec = composite.reconfigure("nn", DatasetProperties.EMPTY, spec);
    Assert.assertEquals(spec, respec);
    try {
      composite.reconfigure("nn", DatasetProperties.builder().add("immutable", "x").build(), spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }
  }

  @Test
  public void testTimeSeriesReconfigure() throws IncompatibleUpdateException {
    testTimeSeriesReconfigure(registry.get(TimeseriesTable.class.getName()));
    testTimeSeriesReconfigure(registry.get(CounterTimeseriesTable.class.getName()));
  }

  private void testTimeSeriesReconfigure(DatasetDefinition def) throws IncompatibleUpdateException {
    DatasetProperties props = DatasetProperties.builder()
      .add(TimeseriesDataset.ATTR_TIME_INTERVAL_TO_STORE_PER_ROW,
           String.valueOf(TimeUnit.HOURS.toMillis(1)))
      .build();
    DatasetProperties compatProps = TableProperties.builder()
      .setTTL(TimeUnit.DAYS.toSeconds(1))
      .add(TimeseriesDataset.ATTR_TIME_INTERVAL_TO_STORE_PER_ROW, String.valueOf(TimeUnit.HOURS.toMillis(1)))
      .build();
    DatasetProperties incompatProps = TableProperties.builder()
      .setTTL(TimeUnit.DAYS.toSeconds(1))
      .add(TimeseriesDataset.ATTR_TIME_INTERVAL_TO_STORE_PER_ROW, String.valueOf(TimeUnit.HOURS.toMillis(2)))
      .build();
    DatasetSpecification spec = def.configure("tt", props);
    Assert.assertTrue(def instanceof Reconfigurable);
    ((Reconfigurable) def).reconfigure("tt", compatProps, spec);
    try {
      ((Reconfigurable) def).reconfigure("tt", incompatProps, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }
  }

  @Test
  public void testFileSetReconfigure() throws IncompatibleUpdateException {
    testFileSetReconfigure(registry.get(FileSet.class.getName()));
    testFileSetReconfigure(registry.get(PartitionedFileSet.class.getName()),
                           PartitionedFileSetProperties.builder().setPartitioning(
                             Partitioning.builder().addIntField("i").build()).build());
    testFileSetReconfigure(registry.get(TimePartitionedFileSet.class.getName()));
  }

  private void testFileSetReconfigure(DatasetDefinition def) throws IncompatibleUpdateException {
    testFileSetReconfigure(def, DatasetProperties.EMPTY);
  }

  private void testFileSetReconfigure(DatasetDefinition def,
                                      DatasetProperties extraProps) throws IncompatibleUpdateException {
    // positive case: internal by default <-> explicitly inyternal
    testFileSetReconfigure(true, def, null, null, false, null, extraProps);
    testFileSetReconfigure(true, def, false, null, null, null, extraProps);
    // positive: internal to external with path change from default path
    testFileSetReconfigure(true, def, null, null, true, "/path1", extraProps);
    testFileSetReconfigure(true, def, false, null, true, "/path1", extraProps);
    // positive: internal to external with path change from explicit path
    testFileSetReconfigure(true, def, null, "/path0", true, "/path1", extraProps);
    testFileSetReconfigure(true, def, false, "/path0", true, "/path1", extraProps);

    // negative: internal with path change
    testFileSetReconfigure(false, def, null, null, false, "/path1", extraProps);
    testFileSetReconfigure(false, def, false, null, null, "/path1", extraProps);
    testFileSetReconfigure(false, def, null, "/path0", false, "/path1", extraProps);
    testFileSetReconfigure(false, def, false, "/path0", null, "/path1", extraProps);
    testFileSetReconfigure(false, def, null, "/path0", false, null, extraProps);
    testFileSetReconfigure(false, def, false, "/path0", null, null, extraProps);
    // negative: external to internal
    testFileSetReconfigure(false, def, true, "/path0", false, "/path0", extraProps);
    testFileSetReconfigure(false, def, true, "/path0", null, "/path0", extraProps);
    testFileSetReconfigure(false, def, true, "/path0", null, null, extraProps);
    // negative: internal to external without path chaange
    testFileSetReconfigure(true, def, null, null, true, null, extraProps);
    testFileSetReconfigure(true, def, null, "/path0", true, "/path0", extraProps);
  }

  private void testFileSetReconfigure(boolean expectSuccess, DatasetDefinition def,
                                      Boolean wasExternal, String path,
                                      Boolean newExternal, String newPath,
                                      DatasetProperties extraProps) throws IncompatibleUpdateException {
    Assert.assertTrue(def instanceof Reconfigurable);
    DatasetProperties props = buildFileSetProps(extraProps, wasExternal, path);
    DatasetProperties newProps = buildFileSetProps(extraProps, newExternal, newPath);
    DatasetSpecification spec = def.configure("fs", props);
    if (expectSuccess) {
      ((Reconfigurable) def).reconfigure("fs", newProps, spec);
    } else {
      try {
        ((Reconfigurable) def).reconfigure("fs", newProps, spec);
        Assert.fail("reconfigure should have thrown exception");
      } catch (IncompatibleUpdateException e) {
        // expected
      }
    }
  }

  private DatasetProperties buildFileSetProps(DatasetProperties extraProps, Boolean external, String path) {
    FileSetProperties.Builder builder = FileSetProperties.builder();
    builder.addAll(extraProps.getProperties());
    if (external != null) {
      builder.setDataExternal(external);
    }
    if (path != null) {
      builder.setBasePath(path);
    }
    return builder.build();
  }

  @Test
  public void testPFSReconfigure() throws IncompatibleUpdateException {
    DatasetDefinition pfsDef = registry.get(PartitionedFileSet.class.getName());
    Assert.assertTrue(pfsDef instanceof Reconfigurable);

    DatasetProperties props = PartitionedFileSetProperties.builder().setPartitioning(
      Partitioning.builder().addIntField("i").addStringField("s").build()).build();
    DatasetSpecification spec = pfsDef.configure("pfs", props);

    DatasetProperties noIprops = PartitionedFileSetProperties.builder().setPartitioning(
      Partitioning.builder().addStringField("s").build()).build();
    try {
      ((Reconfigurable) pfsDef).reconfigure("pfs", noIprops, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }

    DatasetProperties longIprops = PartitionedFileSetProperties.builder().setPartitioning(
      Partitioning.builder().addLongField("i").addStringField("s").build()).build();
    try {
      ((Reconfigurable) pfsDef).reconfigure("pfs", longIprops, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }

    DatasetProperties revProps = PartitionedFileSetProperties.builder().setPartitioning(
      Partitioning.builder().addStringField("s").addIntField("i").build()).build();
    try {
      ((Reconfigurable) pfsDef).reconfigure("pfs", revProps, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }

    // test reconfiguring a PFS created before CDAP-13120, with no defaulted base path
    // no base path should be set for the reconfigured dataset either
    DatasetProperties oldProps = PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addStringField("s").build())
      .add(PartitionedFileSetDefinition.NAME_AS_BASE_PATH_DEFAULT, "false")
      .build();
    DatasetSpecification oldSpec = pfsDef.configure("pfs", oldProps);
    DatasetSpecification newSpec = ((Reconfigurable) pfsDef).reconfigure("pfs", oldProps, oldSpec);
    // make sure base path is not set
    Assert.assertNull(newSpec.getSpecification("files").getProperty(FileSetProperties.BASE_PATH));

    // test reconfiguring a PFS created after CDAP-13120, where base path is default to the dataset name
    props = PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addStringField("s").build())
      .build();
    oldSpec = pfsDef.configure("pfs", props);
    newSpec = ((Reconfigurable) pfsDef).reconfigure("pfs", props, oldSpec);
    // make sure base path is similarly set, even when not explicitly given
    Assert.assertEquals("pfs", newSpec.getSpecification("files").getProperty(FileSetProperties.BASE_PATH));

    // make sure it is set for subsequent reconfigures as well
    newSpec = ((Reconfigurable) pfsDef).reconfigure("pfs", props, oldSpec);
    // make sure base path is similarly set, even when not explicitly given
    Assert.assertEquals("pfs", newSpec.getSpecification("files").getProperty(FileSetProperties.BASE_PATH));
  }

  @Test
  public void testIndexedTableReconfigure() throws IncompatibleUpdateException {
    DatasetDefinition indexedTableDef = registry.get(IndexedTable.class.getName());
    Assert.assertTrue(indexedTableDef instanceof Reconfigurable);

    DatasetProperties props = TableProperties.builder()
      .setReadlessIncrementSupport(false)
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "a,b,c")
      .build();
    DatasetSpecification spec = indexedTableDef.configure("idxtb", props);

    DatasetProperties compat = TableProperties.builder()
      .setReadlessIncrementSupport(true) // turning on is ok
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "c,b,a")
      .build();
    spec = ((Reconfigurable) indexedTableDef).reconfigure("idxtb", compat, spec);

    DatasetProperties incompat = TableProperties.builder()
      .setReadlessIncrementSupport(true)
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "a,d")
      .build();
    try {
      ((Reconfigurable) indexedTableDef).reconfigure("idxtb", incompat, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }

    incompat = TableProperties.builder()
      .setReadlessIncrementSupport(false) // turning off is not ok
      .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "a,b,c")
      .build();
    try {
      ((Reconfigurable) indexedTableDef).reconfigure("idxtb", incompat, spec);
      Assert.fail("reconfigure should have thrown exception");
    } catch (IncompatibleUpdateException e) {
      // expected
    }
  }
}

class DatasetDefinitionRegistryWithDefaultModules extends DefaultDatasetDefinitionRegistry {

  @Inject
  DatasetDefinitionRegistryWithDefaultModules(
    Injector injector, @Constants.Dataset.Manager.DefaultDatasetModules Map<String, DatasetModule> defaultModules) {

    injector.injectMembers(this);
    for (Map.Entry<String, DatasetModule> entry : defaultModules.entrySet()) {
      entry.getValue().register(this);
    }
  }
}
