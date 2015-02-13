/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;

import java.util.List;
import java.util.Map;

/**
 * This class was previously used internally by CDAP for a generic table dataset interface.  Its implementation
 * has been replaced and is no longer used.  However, since it exposed constants intended for application use,
 * the class is retained in order to deprecate the exposed constants.
 *
 * @deprecated since 2.8.0.  Use {@link Table} instead.
 */
@Deprecated
public interface OrderedTable extends Dataset {

  /**
   * @deprecated since 2.8.0. Use {@link Table#PROPERTY_TTL} instead.
   */
  @Deprecated
  String PROPERTY_TTL = Table.PROPERTY_TTL;

  /**
   * @deprecated since 2.8.0. Use {@link Table#PROPERTY_READLESS_INCREMENT} instead.
   */
  @Deprecated
  String PROPERTY_READLESS_INCREMENT = Table.PROPERTY_READLESS_INCREMENT;
}
