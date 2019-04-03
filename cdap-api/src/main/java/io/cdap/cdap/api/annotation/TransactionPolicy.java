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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.RuntimeContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link ProgramLifecycle} method to indicate that it will not run inside transaction.
 * 
 * By default, program life cycle methods ({@link ProgramLifecycle#initialize(RuntimeContext) initialize},
 * {@link ProgramLifecycle#destroy() destroy}), are run inside a transaction. Some methods do not require a
 * transaction, or they need to control their transactions themselves. Such a method can be annotated,
 * for example in a worker, with:
 *
 * <pre><code>
 * {@literal @}Override
 * {@literal @}TransactionPolicy(TransactionControl.EXPLICIT)
 * public void initialize(WorkerContext context) {
 *   ...
 * }
 * </code></pre>
 *
 * This is especially useful if the method may run longer than the transaction timeout, and it would then
 * fail if run inside a transaction.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TransactionPolicy {

  TransactionControl value();
}
