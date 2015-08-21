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
package co.cask.cdap.common.options;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// Due to a bug in checkstyle, it would emit false positives here of the form
// "Unused Javadoc tag (line:col)" for each of the default clauses.
// This comment disables that check up to the corresponding ON comments below

// CHECKSTYLE OFF: Unused Javadoc tag

/**
 * Options for command line.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Option {
  /**
   * Specifies the name of the command line option to be associated with the variable of a class.
   * @return name associated with class variable.
   */
  String name() default "";

  /**
   * Specifies the usage.
   * @return usage info.
   */
  String usage() default "";

  /**
   * Specifies the type of parameter.
   * @return type parameter.
   */
  String type() default "";

  /**
   * Specifies whether a variable annotated with @Option need to be displayed as part of usage.
   * @return true to hide; false otherwise.
   */
  boolean hidden() default false;

  /**
   * Specifies the environment variable to be associated a field.
   * @return environment variable to be read for a field.
   */
  String envVar() default "";
}

// CHECKSTYLE ON
