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

package co.cask.cdap.common.security;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for a method that needs Authorization enforcement.
 * <p>
 * {@link AuthEnforce#entities()}: Specifies the entity on which authorization will be enforced.
 * Method parameters and class variables which are needed as entities should be marked with {@link Name} annotation
 * with a unique name and those names should be provided here.
 * It can either be an EntityId or an array of Strings from which the EntityId on which enforcement is needed
 * (specified through {@link AuthEnforce#enforceOn()}) can be constructed.
 * This will first be looked up in method parameter and if not found it will be looked up in the class member variable.
 * If a class member variable is marked with the same name as a method parameter then the method parameter will take
 * precedence over the class member variable. If you you want the class member variable to be used then specify it
 * with this.AnnotationName
 * <p>
 * {@link AuthEnforce#enforceOn()}: CDAP entities (see {@link EntityId}) class on which enforcement will be done. If
 * you want to enforce on the parent of the entity specify that EntityId class here
 * <p>
 * {@link AuthEnforce#actions()}: An array of {@link Action} to be checked during enforcement
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthEnforce {

  /**
   * Specifies the entity on which authorization will be enforced. Method parameters and class variables which are
   * needed as entities should be marked with {@link Name} annotation with a unique name and those names should be
   * provided here. It can either be an EntityId or an array of Strings from which the EntityId on which enforcement
   * is needed (specified through {@link AuthEnforce#enforceOn()}) can be constructed. This will first be looked up
   * in method parameter and if not found it will be looked up in the class member variable. If a class member
   * variable is marked with the same name as a method parameter then the method parameter will take precedence over
   * the class member variable. If you you want the class member variable to be used then specify it with
   * this.AnnotationName
   */
  String[] entities();

  /**
   * CDAP entities (see {@link EntityId}) class on which enforcement will be done. If
   * you want to enforce on the parent of the entity specify that EntityId class here
   */
  Class<? extends EntityId> enforceOn();

  /**
   * An array of {@link Action} to be checked during enforcement
   */
  Action[] actions();
}
