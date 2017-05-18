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

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.common.BasicErrorRecord;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * An emitter used in Spark to collect all output and errors emitted.
 *
 * @param <T> the type of object to emit
 */
public class CombinedEmitter<T> implements Emitter<T> {
  private final String stageName;
  private final List<Tuple2<Boolean, Object>> emitted = new ArrayList<>();

  public CombinedEmitter(String stageName) {
    this.stageName = stageName;
  }

  @Override
  public void emit(T value) {
    emitted.add(new Tuple2<Boolean, Object>(false, value));
  }

  @Override
  public void emitError(InvalidEntry<T> invalidEntry) {
    ErrorRecord<T> errorRecord = new BasicErrorRecord<>(invalidEntry.getInvalidRecord(), stageName,
                                                        invalidEntry.getErrorCode(), invalidEntry.getErrorMsg());
    emitted.add(new Tuple2<Boolean, Object>(true, errorRecord));
  }

  /**
   * @return all output and errors emitted. If the first val is true, it is an error. Otherwise it is an output.
   */
  public Iterable<Tuple2<Boolean, Object>> getEmitted() {
    return emitted;
  }

  public void reset() {
    emitted.clear();
  }
}
