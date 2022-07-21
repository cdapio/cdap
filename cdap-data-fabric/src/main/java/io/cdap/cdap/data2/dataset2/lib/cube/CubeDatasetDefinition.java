/*
 * Copyright 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.cube;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.Reconfigurable;
import io.cdap.cdap.api.dataset.lib.AbstractDatasetDefinition;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dataset definition of {@link CubeDataset}, the dataset that implements {@link io.cdap.cdap.api.dataset.lib.cube.Cube}
 * to store and query {@link io.cdap.cdap.api.dataset.lib.cube.CubeFact}s.
 * <p/>
 * Cube dataset can be configured with aggregation resolutions and aggregations. E.g.
    <pre>
     dataset.cube.resolutions=1,60
     dataset.cube.aggregation.userPages.dimensions=user,page
     dataset.cube.aggregation.userPages.requiredDimensions=page
     dataset.cube.aggregation.userActions.dimensions=user,action
     dataset.cube.aggregation.userActions.requiredDimensions=action
     dataset.cube.coarse.lag.factor=5
     dataset.cube.coarse.round.factor=4
    </pre>
 *
 * <ul>
 *   <li>
 *     configures Cube to aggregate data for 1 second and 60 seconds resolutions
 *   </li>
 *   <li>
 *     configures "userPages" aggregation (name doesn't have any restricted format, can be any alphabetical) that
 *     aggregates measurements for user and page; allows querying e.g. number of visits of specific user of specific
 *     page
 *   </li>
 *   <li>
 *     configures "userActions" aggregation (name doesn't have any restricted format, can be any alphabetical) that
 *     aggregates measurements for user and action; allows querying e.g. number of specific actions of specific user
 *   </li>
 *   <li>
 *     configures coarsing to kick in after 5x delay (5 and 300 seconds for resolutions) and additionally
 *     aggregare data to 4x more sparce values (4 and 240 seconds interval).
 *   </li>
 * </ul>
 *
 * Aggregation is defined with list of dimensions to aggregate by and a list of required dimensions
 * (dataset.cube.aggregation.[agg_name].dimensions and dataset.cube.aggregation.[agg_name].requiredDimensions properties
 * respectively). The {@link io.cdap.cdap.api.dataset.lib.cube.CubeFact} measurement is aggregated within an aggregation
 * if it contains all required dimensions which non-null value.
 */
public class CubeDatasetDefinition
  extends AbstractDatasetDefinition<CubeDataset, DatasetAdmin>
  implements Reconfigurable {

  public static final String PROPERTY_AGGREGATION_PREFIX = "dataset.cube.aggregation.";
  public static final String PROPERTY_DIMENSIONS = "dimensions";
  public static final String PROPERTY_REQUIRED_DIMENSIONS = "requiredDimensions";
  public static final String PROPERTY_COARSE_LAG_FACTOR = "dataset.cube.coarse.lag.factor";
  public static final String PROPERTY_COARSE_ROUND_FACTOR = "dataset.cube.coarse.round.factor";
  // Coarsing disabled by default
  public static final int DEFAULT_COARSE_ROUND_FACTOR = 1;
  // If only round factor is specified, lag factor of 10 will be used
  public static final int DEFAULT_COARSE_LAG_FACTOR = 10;
  // 1 second is the only default resolution
  public static final int[] DEFAULT_RESOLUTIONS = new int[]{1};

  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends Table, ?> tableDef;
  private final DatasetDefinition<MetricsTable, ? extends DatasetAdmin> metricsTableDef;

  /**
   * Creates instance of {@link CubeDatasetDefinition}.
   *
   * @param name this dataset type name
   * @param tableDef {@link Table} dataset definition, used to create tables to store cube data
   * @param metricsTableDef {@link MetricsTable} dataset definition, used to create tables to store encoding mappings
   *                        (see "entity" table).
   *                        Has to be non-transactional: Cube dataset uses in-memory non-transactional cache in front
   *                        of it, so all writes must be durable independent on transaction completion.
   */
  public CubeDatasetDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef,
                               DatasetDefinition<MetricsTable, ? extends DatasetAdmin> metricsTableDef) {
    super(name);
    this.tableDef = tableDef;
    this.metricsTableDef = metricsTableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {

    DatasetProperties factTableProperties = computeFactTableProperties(properties);
    List<DatasetSpecification> datasetSpecs = Lists.newArrayList();

    // Configuring table that hold mappings of tag names and values and such
    datasetSpecs.add(metricsTableDef.configure("entity", properties));

    // NOTE: we create a table per resolution; we later will use that to e.g. configure ttl separately for each
    for (int resolution : getResolutions(properties.getProperties())) {
      datasetSpecs.add(tableDef.configure(String.valueOf(resolution), factTableProperties));
    }

    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(datasetSpecs)
      .build();
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProps,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {

    DatasetProperties factTableProperties = computeFactTableProperties(newProps);
    List<DatasetSpecification> datasetSpecs = Lists.newArrayList();

    // Configuring table that hold mappings of tag names and values and such
    datasetSpecs.add(reconfigure(metricsTableDef, "entity", newProps, currentSpec.getSpecification("entity")));

    for (int resolution : getResolutions(newProps.getProperties())) {
      String factTableName = String.valueOf(resolution);
      DatasetSpecification existing = currentSpec.getSpecification(factTableName);
      DatasetSpecification factTableSpec = existing == null
        ? tableDef.configure(factTableName, factTableProperties)
        : reconfigure(tableDef, factTableName, factTableProperties, existing);
      datasetSpecs.add(factTableSpec);
    }

    return DatasetSpecification.builder(instanceName, getName())
      .properties(newProps.getProperties())
      .datasets(datasetSpecs)
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    Map<String, DatasetAdmin> admins = new HashMap<>();

    admins.put("entity", metricsTableDef.getAdmin(datasetContext, spec.getSpecification("entity"), classLoader));

    int[] resolutions = getResolutions(spec.getProperties());
    for (int resolution : resolutions) {
      String resolutionTable = String.valueOf(resolution);
      admins.put(resolutionTable,
                 tableDef.getAdmin(datasetContext, spec.getSpecification(resolutionTable), classLoader));
    }
    return new CubeDatasetAdmin(spec, admins);
  }

  @Override
  public CubeDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    MetricsTable entityTable =
      metricsTableDef.getDataset(datasetContext, spec.getSpecification("entity"), arguments, classLoader);

    int[] resolutions = getResolutions(spec.getProperties());
    Map<Integer, Table> resolutionTables = Maps.newHashMap();
    for (int resolution : resolutions) {
      resolutionTables.put(resolution,
                           tableDef.getDataset(datasetContext, spec.getSpecification(String.valueOf(resolution)),
                                               arguments, classLoader));
    }

    Map<String, Aggregation> aggregations = getAggregations(spec.getProperties());

    int coarseLagFactor = spec.getIntProperty(PROPERTY_COARSE_LAG_FACTOR, DEFAULT_COARSE_LAG_FACTOR);
    int coarseRoundFactor = spec.getIntProperty(PROPERTY_COARSE_ROUND_FACTOR, DEFAULT_COARSE_ROUND_FACTOR);

    return new CubeDataset(spec.getName(), entityTable, resolutionTables, aggregations,
                           coarseLagFactor, coarseRoundFactor);
  }

  private DatasetProperties computeFactTableProperties(DatasetProperties props) {
    // Configuring tables that hold data of specific resolution
    Map<String, Aggregation> aggregations = getAggregations(props.getProperties());
    // Adding pre-splitting for fact tables
    byte[][] splits = FactTable.getSplits(aggregations.size());
    // and combine them
    return DatasetProperties.builder()
      .addAll(props.getProperties())
      .add(HBaseTableAdmin.PROPERTY_SPLITS, GSON.toJson(splits))
      .build();
  }

  private Map<String, Aggregation> getAggregations(Map<String, String> properties) {
    // Example of configuring one aggregation with two dimensions: user and action and user being required:
    //   dataset.cube.aggregation.1.dimensions=user,action
    //   dataset.cube.aggregation.1.requiredDimensions=user

    Map<String, List<String>> aggDimensions = Maps.newHashMap();
    Map<String, Set<String>> aggRequiredDimensions = Maps.newHashMap();
    for (Map.Entry<String, String> prop : properties.entrySet()) {
      if (prop.getKey().startsWith(PROPERTY_AGGREGATION_PREFIX)) {
        String aggregationProp = prop.getKey().substring(PROPERTY_AGGREGATION_PREFIX.length());
        String[] nameAndProp = aggregationProp.split("\\.", 2);
        if (nameAndProp.length != 2) {
          throw new IllegalArgumentException("Invalid property: " + prop.getKey());
        }
        String[] dimensions = prop.getValue().split(",");
        if (PROPERTY_DIMENSIONS.equals(nameAndProp[1])) {
          aggDimensions.put(nameAndProp[0], Arrays.asList(dimensions));
        } else if (PROPERTY_REQUIRED_DIMENSIONS.equals(nameAndProp[1])) {
          aggRequiredDimensions.put(nameAndProp[0], new HashSet<>(Arrays.asList(dimensions)));
        } else {
          throw new IllegalArgumentException("Invalid property: " + prop.getKey());
        }
      }
    }

    Map<String, Aggregation> aggregations = Maps.newHashMap();
    for (Map.Entry<String, List<String>> aggDimensionsEntry : aggDimensions.entrySet()) {
      Set<String> requiredDimensions = aggRequiredDimensions.get(aggDimensionsEntry.getKey());
      requiredDimensions = requiredDimensions == null ? Collections.<String>emptySet() : requiredDimensions;
      aggregations.put(aggDimensionsEntry.getKey(),
                       new DefaultAggregation(aggDimensionsEntry.getValue(), requiredDimensions));
    }
    return aggregations;
  }

  private int[] getResolutions(Map<String, String> propsMap) {
    // Example of configuring 1 second and 60 seconds resolutions:
    //   dataset.cube.resolutions=1,60

    String resProp = propsMap.get(Cube.PROPERTY_RESOLUTIONS);
    int[] resolutions;
    if (resProp == null) {
      resolutions = DEFAULT_RESOLUTIONS;
    } else {
      String[] seconds = resProp.split(",");
      if (seconds.length == 0) {
        throw new IllegalArgumentException(String.format("Invalid value %s for property %s.",
                                                         resProp, Cube.PROPERTY_RESOLUTIONS));
      }
      resolutions = new int[seconds.length];
      for (int i = 0; i < seconds.length; i++) {
        try {
          resolutions[i] = Integer.valueOf(seconds[i]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(String.format("Invalid resolution value %s in property %s.",
                                                           seconds[i], Cube.PROPERTY_RESOLUTIONS));
        }
      }
    }
    return resolutions;
  }
}
