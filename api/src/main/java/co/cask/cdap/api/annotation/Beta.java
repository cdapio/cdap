/*
 * Copyright 2014 Cask, Inc.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a public API that may be subjected to incompatible changes up to the extent of removal from
 * the library in future releases.
 *
 * <p>
 * This annotation, if declared on an API, exempts it from compatibility guarantees made by its library.
 * </p>
 *
 * <p>
 * Note that having this annotation does <b>not</b> imply differences in quality with APIs 
 * that are non-beta, nor does it imply that they are inferior in terms of performance
 * with their non-beta counterparts. This annotation just signifies that the
 * APIs have not been finalized and could be subject to change in the future.
 * </p>
 */
@Retention(RetentionPolicy.CLASS)
@Target({
          ElementType.ANNOTATION_TYPE,
          ElementType.CONSTRUCTOR,
          ElementType.FIELD,
          ElementType.METHOD,
          ElementType.TYPE})
@Documented
public @interface Beta {
}
