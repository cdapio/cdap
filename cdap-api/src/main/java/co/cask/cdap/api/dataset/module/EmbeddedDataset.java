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

package co.cask.cdap.api.dataset.module;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a parameter to be used in {@link co.cask.cdap.api.dataset.Dataset} constructor
 * which will be injected with {@link co.cask.cdap.api.dataset.Dataset} instance.
 *
 * See
 * {@link co.cask.cdap.api.app.ApplicationConfigurer#addDatasetType
 * co.cask.cdap.api.app.ApplicationConfigurer#addDatasetType(Class &lt;&#63; extends Dataset&gt; datasetClass)
 * }
 * for details. 
 *
 * Example of usage:
 *
 * <pre>
 * <code>
 *  public class SimpleKVTable extends AbstractDataset {
 *    public SimpleKVTable(DatasetSpecification spec, {@literal @}EmbeddedDataset("data") Table table) {
 *      super(spec.getTransactionAwareName(), table);
 *    }
 *
 *    //...
 *  }
 * </code>
 * </pre>
 *
 * Here, upon creation the table parameter will be a dataset that points to a embedded dataset of name "data"
 * (namespaced with this dataset name).
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface EmbeddedDataset {

  static final String DEFAULT_TYPE_NAME = "";

  /**
   * Returns name of the dataset.
   */
  String value();

  /**
   * Optionally returns name of the type of the underlying dataset. If not set, then type of the parameter will be used
   * to resolve it
   */
  String type() default DEFAULT_TYPE_NAME;
}
