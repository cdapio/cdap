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

/**
 * Metadata about a model.
 */
public class ModelMeta {
  private final int numFeatures;
  private final long numPredictions;
  private final long numPredictionsCorrect;
  private final long numPredictionsWrong;
  private final double rmse;
  private final double trainingPercentage;

  public ModelMeta(int numFeatures, long numPredictions, long numCorrect,  double rmse,
                   double trainingPercentage) {
    this.numFeatures = numFeatures;
    this.numPredictions = numPredictions;
    this.numPredictionsCorrect = numCorrect;
    this.numPredictionsWrong = numPredictions - numCorrect;
    this.rmse = rmse;
    this.trainingPercentage = trainingPercentage;
  }

  public int getNumFeatures() {
    return numFeatures;
  }

  public long getNumPredictions() {
    return numPredictions;
  }

  public long getNumPredictionsCorrect() {
    return numPredictionsCorrect;
  }

  public long getNumPredictionsWrong() {
    return numPredictionsWrong;
  }

  public double getRmse() {
    return rmse;
  }

  public double getTrainingPercentage() {
    return trainingPercentage;
  }
}
