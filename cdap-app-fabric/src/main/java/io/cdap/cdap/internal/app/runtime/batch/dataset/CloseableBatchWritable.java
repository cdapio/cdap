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

package io.cdap.cdap.internal.app.runtime.batch.dataset;

import io.cdap.cdap.api.data.batch.BatchWritable;
import java.io.Closeable;

/**
 * A {@link BatchWritable} that is also {@link Closeable}.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public interface CloseableBatchWritable<K, V> extends BatchWritable<K, V>, Closeable {

}
