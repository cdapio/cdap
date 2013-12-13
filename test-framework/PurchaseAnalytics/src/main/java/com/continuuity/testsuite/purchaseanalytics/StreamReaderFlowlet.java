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

package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.testsuite.purchaseanalytics.datamodel.SerializedObject;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

/**
 * Reads and routes all transactions types to the corresponding object store flowlet.
 */
public class StreamReaderFlowlet extends AbstractFlowlet {

  private final Gson gson = new Gson();

  @Output("outPurchase")
  private OutputEmitter<String> outPurchase;

  @Output("outProduct")
  private OutputEmitter<String> outProduct;

  @Output("outCustomer")
  private OutputEmitter<String> outCustomer;

  public void process(StreamEvent event) {

    String body = new String(event.getBody().array());

    try {
      SerializedObject serializedObject = this.gson.fromJson(body, SerializedObject.class);

      if (serializedObject.getType() == 1) {
        outPurchase.emit(body);
      } else if (serializedObject.getType() == 2) {
        outProduct.emit(body);
      } else if (serializedObject.getType() == 3) {
        outCustomer.emit(body);
      } else {
        // TODO: handle unknown object type
      }
    } catch (JsonParseException jpe) {
      throw jpe;
    } finally {
    }
  }
}
