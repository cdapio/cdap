/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotates the requirements needed by a plugin to run successfully.</p>
 *
 * <p>Requirements are case insensitive.</p>
 *
 * <p>If a plugin is not annotated with {@link Requirements} or the annotation {@link #datasetTypes()} is empty,
 * then it is assumed that the plugin does not have any specific requirements and can run everywhere.</p>
 *
 * <p>Usage Examples:</p>
 * <ul>
 * <li><b>Omitting Requirements:</b> A plugin can choose to not specify any requirement by not specifying a
 * {@link Requirements} annotation. In this case the plugin will considered to be capable or run everywhere.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("CSVParser")
 *      public class CSVParser extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * <li><b>Specifying a particular requirement:</b> If a plugin requires CDAP 'table' dataset then this can be specified
 * as below.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("Table")
 *     {@literal @}Requirements(datasetTypes = {"table"})
 *      public class Table extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * <li><b>Specifying multiple requirements:</b> A plugin can also specify multiple requirements. For example if a
 * plugin needs 'table' and 'keyValueTable' to run this can specified as below.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("MultiTable")
 *     {@literal @}Requirements(datasetTypes = {"table", "keyValueTable"})
 *      public class MultiTableSource extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Requirements {

  String[] datasetTypes() default {};

  /**
   * Names of associated add-ons or capabilities
   * @return String array of capability names
   */
  String[] capabilities() default{};
}
