/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;
import org.apache.twill.filesystem.Location;

import java.util.List;

/**
 * This dataset represents a collection of files on the file system. The dataset has a base location, under which
 * all of its files are located. When instantiated, runtime arguments are required to specify the individual file
 * or files being used.
 */
@Beta
public interface File extends Dataset, InputFormatProvider, OutputFormatProvider {

  /**
   * The base path of the dataset.
   */
  String PROPERTY_BASE_PATH = "base.path";

  /**
   * The name of the input format class.
   */
  String PROPERTY_INPUT_FORMAT = "input.format";

  /**
   * The name of the output format class.
   */
  String PROPERTY_OUTPUT_FORMAT = "output.format";

  /**
   * Prefix for additional properties for the input format. They are added to the
   * Hadoop configuration, with the prefix stripped from the name.
   */
  String PROPERTY_INPUT_PROPERTIES_PREFIX = "input.properties.";

  /**
   * Prefix for additional properties for the output format. They are added to the
   * Hadoop configuration, with the prefix stripped from the name.
   */
  String PROPERTY_OUTPUT_PROPERTIES_PREFIX = "output.properties.";

  /**
   * The paths of the files to be read. Specified as a runtime argument for the dataset.
   * Each path is relative to the dataset's base path, and multiple paths can be given,
   * separated by commas.
   */
  String ARGUMENT_INPUT_PATHS = "input.paths";

  /**
   * The path of the file to write. Specified as a runtime argument for the dataset.
   * The path is relative to the dataset's base path.
   */
  String ARGUMENT_OUTPUT_PATH = "output.path";

  /**
   * Allows to interact directly with the location of this dataset in the underlying file system.
   *
   * @return the location of the base directory
   */
  Location getBaseLocation();

  /**
   * Allows direct access to files of this dataset in the underlying file system.
   *
   * @return the list of input locations
   */
  List<Location> getInputLocations();

  /**
   * Allows direct access to files in the output location, in the underlying file system.
   *
   * @return the output location
   */
  Location getOutputLocation();

  /**
   * Allows direct access to files in the input locations, in the underlying file system.
   *
   * @return the location of the given relative path within this dataset
   */
  Location getLocation(String relativePath);
}
