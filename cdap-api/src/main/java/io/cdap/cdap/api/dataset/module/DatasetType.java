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

package io.cdap.cdap.api.dataset.module;

import io.cdap.cdap.api.annotation.Beta;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a dataset type from {@link io.cdap.cdap.api.dataset.Dataset}.
 * The value is used as dataset type name.
 *
 * This is used when creating {@link io.cdap.cdap.api.dataset.DatasetDefinition} from only
 * {@link io.cdap.cdap.api.dataset.Dataset} implementation. See
 * {@link io.cdap.cdap.api.app.ApplicationConfigurer#addDatasetType
 * io.cdap.cdap.api.app.ApplicationConfigurer#addDatasetType(Class &lt;&#63; extends Dataset&gt; datasetClass)
 * }
 * for details.
 *
 * Example of usage:
 *
 * <pre>
 * <code>
 * {@literal @}DatasetType("KVTable")
 * public class SimpleKVTable extends AbstractDataset {
 *   public SimpleKVTable(DatasetSpecification spec, {@literal @}EmbeddedDataset("data") Table table) {
 *     super(spec.getTransactionAwareName(), table);
 *   }
 *
 *   //...
 * }
 * </code>
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Beta
public @interface DatasetType {
  /**
   * Returns name of the dataset type.
   */
  String value();
}
