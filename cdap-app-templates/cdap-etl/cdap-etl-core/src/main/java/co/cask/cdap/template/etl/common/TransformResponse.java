/*
 * Copyright © 2015 Cask Data, Inc.
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

import java.util.Iterator;
import java.util.List;

/**
 * class representing transform response.
 * @param <OUT> represents output type
 */
public class TransformResponse<OUT> {

  private final Iterator<OUT> emittedRecords;
  private final List<TransformError<OUT>> transformError;

  public TransformResponse(Iterator<OUT> emittedRecords, List<TransformError<OUT>> transformError) {
    this.emittedRecords = emittedRecords;
    this.transformError = transformError;
  }

  /**
   * get list of {@link co.cask.cdap.template.etl.common.TransformResponse.TransformError}
   */
  public List<TransformError<OUT>> getErrorRecords() {
    return transformError;
  }

  /**
   * get the iterator for emitter records after executing all the transform stages.
   */
  public Iterator<OUT> getEmittedRecords() {
    return emittedRecords;
  }

  /**
   * TransformError representing errors from each stage
   * @param <OUT>
   */
  public static final class TransformError<OUT> {
    private final String transformId;
    private final Iterator<OUT> errorRecords;

    public TransformError(String transformId, Iterator<OUT> errorRecords) {
      this.transformId = transformId;
      this.errorRecords = errorRecords;
    }

    /**
     * return transformId
     */
    public String getTransformId() {
      return transformId;
    }

    /**
     * return iterator of error records
     */
    public Iterator<OUT> getErrorRecords() {
      return errorRecords;
    }

  }
}
