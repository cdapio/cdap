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

package co.cask.cdap.data2.dataset2.tx;

/**
 * Function executed within transaction
 * @param <CONTEXT_TYPE> type of the {@link TxContext} given to the function {@link #call(TxContext)} method
 * @param <RETURN_TYPE> return type of the {@link #call(TxContext)} method
 */
public interface TxCallable<CONTEXT_TYPE extends TxContext, RETURN_TYPE> {
  RETURN_TYPE call(CONTEXT_TYPE context) throws Exception;
}
