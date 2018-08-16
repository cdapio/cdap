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
package co.cask.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotates capabilities of a plugin.</p>
 *
 * <p>Pre-defined system capabilities are defined in {@link Capabilities}.</p>
 *
 * <p>Plugins can also be annotated with custom value to specify capability with business rules.
 * The capability value is case insensitive.</p>
 *
 * <p>If a plugin does not specify any system defined {@link Capabilities} or the {@link #value()} array is empty or
 * only defines custom capabilities then the default system {@link Capabilities} will be used.</p>
 *
 * <p>According to default system {@link Capabilities} the plugin will be considered to be capable of all system defined
 * {@link Capabilities} but not with any custom user defined capability options.
 * Custom capabilities needs to be specifically defined and their presence or absence does not override the default
 * system defined {@link Capabilities}.</p>
 *
 * <p>Usage Examples:</p>
 * <ul>
 * <li><b>Omitting Capability:</b> A plugin can chose to not specify any capability by not specifying a
 * {@link Capability} annotation. In this case the plugin will considered to be capable with all the system defined
 * {@link Capabilities}.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("AllSystemCapableSource")
 *      public class AllSystemCapableSource extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * <li><b>Specifying a particular system capabilities:</b> If a plugin is capable to run only
 * {@link Capabilities#NATIVE} then this can be specified as below.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("NativeSource")
 *     {@literal @}Capability({Capabilities.NATIVE})
 *      public class NativeSource extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * <li><b>Specifying only custom capabilities:</b> Suppose a plugin is capable of masking
 * Personally Identifiable Information (PII) then it can be annotated to specify this capability. This will define
 * that the plugin is capable of all the system defined {@link Capabilities} and also capable of 'PII'.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("MaskingSource")
 *     {@literal @}Capability({"PII"})
 *      public class MaskingSource extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * <li><b>Specifying system and custom capabilities:</b> It is also possible to specify system and custom capabilities
 * together. For example if a plugin is capable to run only {@link Capabilities#NATIVE} and is also capable of masking
 * Personally Identifiable Information (PII) then it can be annotated to specify these capabilities in the following
 * way.</li>
 * <pre>
 *     {@literal @}Plugin(type = BatchSource.PLUGIN_TYPE)
 *     {@literal @}Name("NativeMaskingSource")
 *     {@literal @}Capability({Capabilities.NATIVE, "PII"})
 *      public class NativeMaskingSource extends{@code BatchSource<byte[], Row, StructuredRecord>} {
 *       ...
 *       ...
 *      }
 *   </pre>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Capability {
  String[] value() default {Capabilities.NATIVE, Capabilities.REMOTE, "spark1_2.10", "spark2_2.11"};
}
