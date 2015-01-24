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
import java.util.Map;

/**
 * This dataset represents a collection of files on the file system. The dataset has a base location, under which
 * all of its files are located. When instantiated, runtime arguments are required to specify the individual file
 * or files being used.
 *
 * This dataset can be made available for querying with SQL (explore). This is enabled through dataset
 * properties when the dataset is created. See {@link co.cask.cdap.api.dataset.lib.FileSetProperties}
 * for details. If it is enabled for explore, a Hive external table will be created when the dataset is
 * created. The Hive table is not partitioned, and therefore querying will only work if every path in the
 * fileset is a file (not a subdirectory).
 */
@Beta
public interface FileSet extends Dataset, InputFormatProvider, OutputFormatProvider {

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
   * Allows direct access to files in this dataset, in the underlying file system.
   *
   * @return the full location given by the path, relative to the base path.
   */
  Location getLocation(String relativePath);

  /**
   * Allow direct access to the runtime arguments of this file set.
   *
   * @return the runtime arguments specified for this file set.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * A variant of {@link co.cask.cdap.api.data.batch.InputFormatProvider#getInputFormatConfiguration}
   * that allows passing in the input locations (rather than using the input locations that were
   * determined from runtime arguments).
   *
   * @param inputLocs the input locations to be used
   */
  Map<String, String> getInputFormatConfiguration(Iterable<? extends Location> inputLocs);

}
