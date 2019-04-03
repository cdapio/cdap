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

package co.cask.cdap.etl.spark;

import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;
import co.cask.cdap.etl.common.BasicErrorRecord;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.RecordType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An emitter used in Spark to collect all output and errors emitted.
 *
 * @param <T> the type of object to emit
 */
public class CombinedEmitter<T> implements Emitter<T>, MultiOutputEmitter<T> {
  private final String stageName;
  private final List<RecordInfo<Object>> emitted = new ArrayList<>();

  public CombinedEmitter(String stageName) {
    this.stageName = stageName;
  }

  @Override
  public void emit(T value) {
    emitted.add(RecordInfo.<Object>builder(value, stageName, RecordType.OUTPUT).build());
  }

  @Override
  public void emit(String port, Object value) {
    emitted.add(RecordInfo.<Object>builder(value, stageName, RecordType.OUTPUT).fromPort(port).build());
  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {
    ErrorRecord<T> errorRecord = new BasicErrorRecord<>(invalidEntry.getInvalidRecord(), stageName,
                                                        invalidEntry.getErrorCode(), invalidEntry.getErrorMsg());
    emitted.add(RecordInfo.<Object>builder(errorRecord, stageName, RecordType.ERROR).build());
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    Alert alert = new Alert(stageName, payload);
    emitted.add(RecordInfo.<Object>builder(alert, stageName, RecordType.ALERT).build());
  }

  /**
   * @return all output and errors emitted.
   */
  public Iterable<RecordInfo<Object>> getEmitted() {
    return emitted;
  }

  public void reset() {
    emitted.clear();
  }

}
