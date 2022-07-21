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

/**
 * Model for REST response of the Spark Server's stages method.
 */
public class SparkApplicationsStagesResponse {

  private String stageId;
  private String status;
  private long inputBytes;
  private int inputRecords;
  private long outputBytes;
  private int outputRecords;
  private int shuffleReadRecords;
  private long shuffleReadBytes;
  private int shuffleWriteRecords;
  private long shuffleWriteBytes;

  public SparkApplicationsStagesResponse() {
  }

  public String getStageId() {
    return stageId;
  }

  public void setStageId(String stageId) {
    this.stageId = stageId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getInputBytes() {
    return inputBytes;
  }

  public void setInputBytes(long inputBytes) {
    this.inputBytes = inputBytes;
  }

  public int getInputRecords() {
    return inputRecords;
  }

  public void setInputRecords(int inputRecords) {
    this.inputRecords = inputRecords;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  public void setOutputBytes(long outputBytes) {
    this.outputBytes = outputBytes;
  }

  public int getOutputRecords() {
    return outputRecords;
  }

  public void setOutputRecords(int outputRecords) {
    this.outputRecords = outputRecords;
  }

  public int getShuffleReadRecords() {
    return shuffleReadRecords;
  }

  public void setShuffleReadRecords(int shuffleReadRecords) {
    this.shuffleReadRecords = shuffleReadRecords;
  }

  public long getShuffleReadBytes() {
    return shuffleReadBytes;
  }

  public void setShuffleReadBytes(long shuffleReadBytes) {
    this.shuffleReadBytes = shuffleReadBytes;
  }

  public int getShuffleWriteRecords() {
    return shuffleWriteRecords;
  }

  public void setShuffleWriteRecords(int shuffleWriteRecords) {
    this.shuffleWriteRecords = shuffleWriteRecords;
  }

  public long getShuffleWriteBytes() {
    return shuffleWriteBytes;
  }

  public void setShuffleWriteBytes(long shuffleWriteBytes) {
    this.shuffleWriteBytes = shuffleWriteBytes;
  }
}
