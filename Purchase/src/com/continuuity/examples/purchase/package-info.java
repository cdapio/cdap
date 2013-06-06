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
