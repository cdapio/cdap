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

package co.cask.cdap.app.runtime.spark.data

import co.cask.cdap.api.data.batch.SplitReader
import org.apache.spark.TaskContext

/**
  * Spark2 SplitReaderIterator, which does not have access to Spark's metrics.
  */
class SplitReaderIterator[K, V](context: TaskContext, splitReader: SplitReader[K, V])
  extends AbstractDatumScannerIterator(context, splitReader) {

}
