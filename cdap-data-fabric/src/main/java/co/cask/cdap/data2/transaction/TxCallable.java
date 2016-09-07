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

package co.cask.cdap.data2.transaction;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;

/**
 * A callable that provides a {@link DatasetContext} to programs which may be used to get
 * access to and use datasets.
 *
 * @param <V> type of the return value from the {@link #call(DatasetContext)}.
 *
 * TODO: CDAP-6103 Move to api and have {@link Transactional} to support this when revamping tx supports in program.
 */
public interface TxCallable<V> {

  V call(DatasetContext context) throws Exception;
}
