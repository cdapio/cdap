/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.templates.plugins.PluginConfig;

import javax.annotation.Nullable;

/**
 * Config class for Cube dataset sinks
 */
public class CubeSinkConfig extends PluginConfig {
  @Description("Name of the Cube dataset. If the Cube does not already exist, one will be created.")
  String name;

  @Name(Properties.Cube.DATASET_RESOLUTIONS)
  @Description("Aggregation resolutions to be used if new Cube dataset " +
    "needs to be created. See Cube dataset configuration " +
    "details for more information : http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/" +
    "cube.html#cube-configuration")
  @Nullable
  String resolutions;

  @Name(Properties.Cube.DATASET_OTHER)
  @Description("Provide any dataset properties to be used if new Cube " +
    "dataset needs to be created as a JSON Map. For example if aggregations are desired on fields - abc and xyz, the " +
    "property should have the value: " +
    "\"{\"dataset.cube.aggregation.agg1.dimensions\":\"abc\", \"dataset.cube.aggregation.agg2.dimensions\":\"xyz\"}." +
    " See Cube dataset configuration " +
    "details for more information : http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/" +
    "cube.html#cube-configuration")
  @Nullable
  String datasetOther;

  @Name(Properties.Cube.FACT_TS_FIELD)
  @Description("Name of the StructuredRecord's field that contains" +
    " timestamp to be used in CubeFact. If not provided, the current time of the record processing will be used as " +
    "CubeFact timestamp.")
  @Nullable
  String tsField;

  @Name(Properties.Cube.FACT_TS_FORMAT)
  @Description("Format of the value of timstamp field, " +
    "e.g. \"HH:mm:ss\" (used if " + Properties.Cube.FACT_TS_FIELD + " is provided).")
  @Nullable
  String tsFormat;

  @Name(Properties.Cube.MEASUREMENTS)
  @Description("Measurements to be extracted from StructuredRecord to be" +
    " used in CubeFact. Provide properties as a JSON Map. For example use 'price' field as a measurement of type" +
    " gauge, and 'count' field as a measurement of type counter, the property should have the value: " +
    "\"{\"cubeFact.measurement.price\":\"GAUGE\", \"cubeFact.measurement.count\":\"COUNTER\"}.")
  @Nullable
  String measurements;

  public CubeSinkConfig(String name, String resolutions, String datasetOther,
                        String tsField, String tsFormat, String measurements) {
    this.name = name;
    this.resolutions = resolutions;
    this.datasetOther = datasetOther;
    this.tsField = tsField;
    this.tsFormat = tsFormat;
    this.measurements = measurements;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getResolutions() {
    return resolutions;
  }

  @Nullable
  public String getDatasetOther() {
    return datasetOther;
  }

  @Nullable
  public String getTsField() {
    return tsField;
  }

  @Nullable
  public String getTsFormat() {
    return tsFormat;
  }

  @Nullable
  public String getMeasurements() {
    return measurements;
  }
}

