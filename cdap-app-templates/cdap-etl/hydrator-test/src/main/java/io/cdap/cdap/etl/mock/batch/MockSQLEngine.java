/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.SQLOperationResult;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;

/**
 * Mock SQL engine that can be used to test join pipelines.
 */
public class MockSQLEngine implements SQLEngine<Object, Object, Object, Object> {
  @Override
  public OutputFormatProvider getPushProvider(SQLPushRequest pushRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public InputFormatProvider getPullProvider(SQLPullRequest pullRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public boolean exists(String datasetName) throws SQLEngineException {
    return false;
  }

  @Override
  public boolean canJoin(SQLJoinRequest joinRequest) {
    return false;
  }

  @Override
  public SQLOperationResult join(SQLJoinRequest joinRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public void cleanup(String datasetName) throws SQLEngineException {

  }

  @Override
  public Transform<StructuredRecord, KeyValue<Object, Object>> toKeyValue() {
    return null;
  }

  @Override
  public Transform<KeyValue<Object, Object>, StructuredRecord> fromKeyValue() {
    return null;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

  }

  @Override
  public void prepareRun(BatchContext context) throws Exception {

  }

  @Override
  public void onRunFinish(boolean succeeded, BatchContext context) {

  }
}
