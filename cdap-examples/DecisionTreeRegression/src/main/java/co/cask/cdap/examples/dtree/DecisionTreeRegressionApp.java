/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.dtree;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;

/**
 * Application that trains a decision tree regression model.
 * Consists of a service used to upload training data and a Spark program model trainer.
 */
public class DecisionTreeRegressionApp extends AbstractApplication {
  public static final String TRAINING_DATASET = "trainingData";
  public static final String MODEL_DATASET = "models";
  public static final String MODEL_META = "modelMeta";

  @Override
  public void configure() {
    addService(new ModelDataService());
    addSpark(new ModelTrainer());

    createDataset(TRAINING_DATASET, FileSet.class.getName(), DatasetProperties.EMPTY);
    createDataset(MODEL_DATASET, FileSet.class.getName(), DatasetProperties.EMPTY);
    try {
      createDataset(MODEL_META, ObjectMappedTable.class.getName(),
                    ObjectMappedTableProperties.builder()
                      .setType(ModelMeta.class)
                      .setRowKeyExploreName("id")
                      .setRowKeyExploreType(Schema.Type.STRING)
                      .setExploreTableName(MODEL_META)
                      .build());
    } catch (UnsupportedTypeException e) {
      // will never happen
      throw new IllegalStateException("ModelMeta has an unsupported schema.", e);
    }
  }
}
