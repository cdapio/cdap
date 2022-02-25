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

package io.cdap.cdap.internal.events.http;

import java.util.ArrayList;

/**
 * Model for REST response of the Spark Server's stages method.
 */
public class SparkApplicationsStagesResponse {
  private String status;
  private int inputBytes;
  private int inputRecords;
  private int outputBytes;
  private int outputRecords;

  public SparkApplicationsStagesResponse() {
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getInputBytes() {
    return inputBytes;
  }

  public void setInputBytes(int inputBytes) {
    this.inputBytes = inputBytes;
  }

  public int getInputRecords() {
    return inputRecords;
  }

  public void setInputRecords(int inputRecords) {
    this.inputRecords = inputRecords;
  }

  public int getOutputBytes() {
    return outputBytes;
  }

  public void setOutputBytes(int outputBytes) {
    this.outputBytes = outputBytes;
  }

  public int getOutputRecords() {
    return outputRecords;
  }

  public void setOutputRecords(int outputRecords) {
    this.outputRecords = outputRecords;
  }
}

