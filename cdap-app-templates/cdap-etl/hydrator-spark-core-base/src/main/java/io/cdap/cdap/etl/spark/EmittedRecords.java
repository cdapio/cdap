/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.common.RecordInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds all records emitted by a stage.
 */
public class EmittedRecords {
  private final SparkCollection<RecordInfo<Object>> rawData;
  private final Map<String, SparkCollection<Object>> outputPortRecords;
  private final SparkCollection<Object> outputRecords;
  private final SparkCollection<ErrorRecord<Object>> errorRecords;
  private final SparkCollection<Alert> alertRecords;

  private EmittedRecords(SparkCollection<RecordInfo<Object>> rawData,
                         Map<String, SparkCollection<Object>> outputPortRecords,
                         SparkCollection<Object> outputRecords,
                         SparkCollection<ErrorRecord<Object>> errorRecords,
                         SparkCollection<Alert> alertRecords) {
    this.rawData = rawData;
    this.outputPortRecords = outputPortRecords;
    this.outputRecords = outputRecords;
    this.errorRecords = errorRecords;
    this.alertRecords = alertRecords;
  }

  public SparkCollection<RecordInfo<Object>> getRawData() {
    return rawData;
  }

  public Map<String, SparkCollection<Object>> getOutputPortRecords() {
    return outputPortRecords;
  }

  public SparkCollection<Object> getOutputRecords() {
    return outputRecords;
  }

  public SparkCollection<ErrorRecord<Object>> getErrorRecords() {
    return errorRecords;
  }

  public SparkCollection<Alert> getAlertRecords() {
    return alertRecords;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private SparkCollection<RecordInfo<Object>> rawData;
    private Map<String, SparkCollection<Object>> outputPortRecords;
    private SparkCollection<Object> outputRecords;
    private SparkCollection<ErrorRecord<Object>> errorRecords;
    private SparkCollection<Alert> alertRecords;

    private Builder() {
      outputPortRecords = new HashMap<>();
    }

    public Builder setRawData(SparkCollection<RecordInfo<Object>> rawData) {
      this.rawData = rawData;
      return this;
    }

    public Builder addPort(String port, SparkCollection<Object> records) {
      outputPortRecords.put(port, records);
      return this;
    }

    public Builder setOutput(SparkCollection<Object> records) {
      outputRecords = records;
      return this;
    }

    public Builder setErrors(SparkCollection<ErrorRecord<Object>> errors) {
      errorRecords = errors;
      return this;
    }

    public Builder setAlerts(SparkCollection<Alert> alerts) {
      alertRecords = alerts;
      return this;
    }

    public EmittedRecords build() {
      return new EmittedRecords(rawData, outputPortRecords, outputRecords, errorRecords, alertRecords);
    }
  }
}
