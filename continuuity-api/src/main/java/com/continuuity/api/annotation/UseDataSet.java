/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare that a method uses a {@link com.continuuity.api.data.DataSet}.
 *
 * <p>
 * Example: Store the incoming Purchase objects in the purchases dataset:
 *
 * <pre>
 *   <code>
 *     public class PurchaseStore extends AbstractFlowlet {
 *     {@literal @}UseDataSet("purchases")
 *     private ObjectStore{@literal <}Purchase> store;
 *
 *     {@literal @}ProcessInput
 *     public void process(Purchase purchase) {
 *       store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
 *     }
 *   </code>
 * </pre>
 * 
 * See the <i>Continuuity Reactor Developer Guide</i> and the example code for the Purchase application.
 * <p>
 * See the <i><a href="http://continuuity.com/docs/reactor/current/en/">Continuuity Reactor Developer Guides</a></i>
 * and the <a href="http://continuuity.com/docs/reactor/current/en/examples/purchase.html">example code for the 
 * Purchase application</a> for more information.
 * </p>
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 * @see com.continuuity.api.procedure.Procedure Procedure
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface UseDataSet {
  /**
   * Returns name of the {@link com.continuuity.api.data.DataSet}.
   */
  String value();
}
