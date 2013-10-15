/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 *
 * An app that uses scheduled MapReduce workflows to read from one ObjectStore dataset and write to another.
 * <ul>
 *   <li>
 *     Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream.
 *   </li><li>
 *     The PurchaseFlow reads the purchaseStream and converts every input String into a Purchase object and stores
 *     the object in the purchases dataset.
 *   </li><li>
 *     When scheduled by the PurchaseHistoryWorkflow, the PurchaseHistoryBuilder MapReduce job reads the purchases dataset, 
 *     creates a purchase history, and stores the purchase history in the history dataset every morning at 4:00 A.M. 
 *     Or you can manually (in the Process screen in the Reactor Dashboard) or programmatically execute 
 *     the PurchaseHistoryBuilder MapReduce job to store customers' purchase history in the history dataset.
 *   </li><li>
 *     Execute the PurchaseQuery procedure to query the history dataset to discover the purchase history of each user.
 *     <p>
 *       Note: Because by default the PurchaseHistoryWorkflow process doesn't run until 4:00 A.M., you'll have to wait 
 *       until the next day (or manually or programmatically execute the PurcaseHistoryBuilder) after entering the first 
 *       customers' purchases or the PurchaseQuery will return a "not found" error.
 *     </p>
 *   </li>
 * </ul>
 */
package com.continuuity.examples.purchase;
