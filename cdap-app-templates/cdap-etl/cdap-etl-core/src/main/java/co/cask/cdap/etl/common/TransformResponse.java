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

package co.cask.cdap.etl.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * class representing transform response.
 * @param <OUT> represents output type
 */
public class TransformResponse<OUT> {
  private final Iterator<OUT> emittedRecords;
  private final Map<String, Collection<OUT>> mapTransformIdToErrorEmitter;

  public TransformResponse(Iterator<OUT> outIterator, Map<String, Collection<OUT>> mapTransformIdToErrorEmitter) {
    this.emittedRecords = outIterator;
    this.mapTransformIdToErrorEmitter = mapTransformIdToErrorEmitter;
  }

  public Iterator<OUT> getEmittedRecords() {
    return emittedRecords;
  }

  public Map<String, Collection<OUT>> getMapTransformIdToErrorEmitter() {
    return mapTransformIdToErrorEmitter;
  }

}
