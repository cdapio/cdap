/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.etl.api.batch;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;

public abstract class BatchAggregation<GROUP_BY, RECORD_TYPE, OUTPUT_TYPE> extends BatchConfigurable<BatchSourceContext>
  implements StageLifecycle<BatchRuntimeContext>, Transformation<RECORD_TYPE, OUTPUT_TYPE> {

  public static final String PROP_GROUP_BY = "groupBy";
  public static final String PROP_FUNCTIONS = "functions";

  public abstract void groupBy(RECORD_TYPE input, Emitter<GROUP_BY> emitter);
  public abstract void aggregate(GROUP_BY groupKey, Iterable<RECORD_TYPE> groupRecords, Emitter<OUTPUT_TYPE> emitter);
}
