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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.ErrorRecord;

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link ErrorRecord}.
 *
 * @param <T> the type of error record
 */
public class BasicErrorRecord<T> implements ErrorRecord<T>, Serializable {
  private static final long serialVersionUID = 3026318232156561080L;
  private final T record;
  private final String stageName;
  private final int errorCode;
  private final String errorMessage;

  public BasicErrorRecord(T record, String stageName, int errorCode, String errorMessage) {
    this.record = record;
    this.stageName = stageName;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  @Override
  public T getRecord() {
    return record;
  }

  @Nullable
  @Override
  public int getErrorCode() {
    return errorCode;
  }

  @Nullable
  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getStageName() {
    return stageName;
  }
}
