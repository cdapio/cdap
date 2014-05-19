/**
 * Copyright 2013-2014 Continuuity, Inc.
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
/**
 *
 * An app that uses scheduled MapReduce Workflows to read from one ObjectStore DataSet and write to another.
 * <ul>
 *   <li>
 *     Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream.
 *   </li><li>
 *     The PurchaseFlow reads the purchaseStream and converts every input String into a Purchase object and stores
 *     the object in the purchases DataSet.
 *   </li><li>
 *     When scheduled by the PurchaseHistoryWorkflow, the PurchaseHistoryBuilder MapReduce job
 *     reads the purchases DataSet, creates a purchase history,
 *     and stores the purchase history in the history DataSet every morning at 4:00 A.M.
 *     Or you can manually (in the Process screen in the Reactor Dashboard) or programmatically execute 
 *     the PurchaseHistoryBuilder MapReduce job to store customers' purchase history in the history DataSet.
 *   </li><li>
 *     Execute the PurchaseQuery procedure to query the history DataSet to discover the purchase history of each user.
 *     <p>
 *       Note: Because by default the PurchaseHistoryWorkflow process doesn't run until 4:00 A.M., you'll have to wait 
 *       until the next day (or manually or programmatically execute the PurcaseHistoryBuilder)
 *       after entering the first customers' purchases or the PurchaseQuery will return a "not found" error.
 *     </p>
 *   </li>
 * </ul>
 */
package com.continuuity.examples.purchase;
