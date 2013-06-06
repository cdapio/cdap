/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * This package contains a simple purchase history application. It illustrates how to use the ObjectStore dataset.
 * <ul>
 *   <li>
 *     A stream named "purchases". You can send sentences of the form "Tom bought 5 apples for $10" to the stream.
 *   </li><li>
 *     A dataset "history". It contains for every user the list of his purchases.
 *   </li><li>
 *     A flow that reads the "purchases" stream and builds the purchase history for every user. It reads the stream
 *     and converts avery input String to a Purchase object, then reads the history for that user from the dataset,
 *     adds the new puchase and writes it back to the dataset.
 *   </li><li>
 *     A procedure that allows to query the purchase history per user.
 *   </li>
 * </ul>
 */
package com.continuuity.examples.purchase;
