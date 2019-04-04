/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
 * Annotation to declare that a method uses a {@link io.cdap.cdap.api.dataset.Dataset}.
 *
 * <p>
 * Example:
 *
 * <pre><code>
 *   public class PurchaseStore extends AbstractHttpServiceHandler {
 *     {@literal @}UseDataSet("myTable")
 *     private ObjectStore{@literal <}Purchase> store;
 *     ...
 *     {@literal @}GET
 *     {@literal @}Path("/get/{key}")
 *     public void get(HttpServiceRequest request, HttpServiceResponder responder,
 *                     {@literal @}PathParam("key") String key) {
 *       Purchase purchase = store.get(key);
 *       responder.sendString(purchase.toString());
 *     }
 *     ...
 *   }
 * </code></pre>
 * 
 * <p>
 * See the 
 * <i><a href="http://docs.cask.co/cdap/current/en/developers-manual/index.html">CDAP Developers' Manual</a></i>
 * for more information.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface UseDataSet {
  /**
   * Returns name of the {@link io.cdap.cdap.api.dataset.Dataset}.
   */
  String value();
}
