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
package co.cask.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to denote if a {@link co.cask.cdap.api.plugin.PluginConfig} field can support macro.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Macro {

  /**
   * Returns whether escaping is enabled. Default is false.
   *
   * When escaping is enabled, any character can be escaped with a backslash '\'. For example, with escaping enabled,
   * '\${val}' would evaluate to '${val}'. With escaping disabled, '\${val}' would result in a lookup of 'val'.
   * If the lookup for 'val' is 'xyz', the entire macro would evaluate to '\xyz'. Before enabling escaping on a field,
   * keep in mind that an escape enabled macro field will behave differently than a non-macro field.
   * In a non-macro field, '\n' will be evaluated as-is. In an escape enabled macro field, '\n' will evaluate
   * to 'n'.
   *
   * When escaping is disabled, certain values cannot be expressed. For example, a literal '${val}' cannot be used,
   * since it will always be interpreted as a macro lookup. Similarly, '${${val}}' will not be a lookup on
   * key '${val}', but will be a lookup on whatever '${val}' evaluates to.
   */
  boolean escapingEnabled() default false;
}
