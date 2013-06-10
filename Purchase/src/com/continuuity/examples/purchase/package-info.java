/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * This package contains a simple purchase history application. It illustrates how to use the ObjectStore dataset.
 * <ul>
 *   <li>
 *     A stream named "purchaseStream". You can send sentences of the form "Tom bought 5 apples for $10" to the stream.
 *   </li><li>
 *     A dataset "history". It contains for every user the list of his purchases.
 *   </li><li>
 *     A flow that reads the "purchaseStream" stream and converts every input String to a Purchase object and stores
 *     it in a dataset called "purchases".
 *
 *     A map-reduce job then reads "purchases" dataset and creates a purchase history and stores it in a dataset called
 *     "history"
 *   </li><li>
 *     A procedure that allows to query the purchase history per user.
 *   </li>
 * </ul>
 */
package com.continuuity.examples.purchase;
