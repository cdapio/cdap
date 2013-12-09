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
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.google.gson.Gson;

/**
 * Generates Purchases, Products, customers and inventory. Used by GeneratedPurchaseAnalyticsFlowlet.
 */
public class TransactionGeneratorFlowlet extends AbstractGeneratorFlowlet {
  private FlowletContext context;

  private static final int MAX_CUSTOMER = 1000;
  private static final int MAX_PRODUCT =  10000;
  private static final int MAX_PURCHASE = 100000;

  Gson gson = new Gson();

  TransactionGeneratorHelper helper;


  @Output("outPurchase")
  private OutputEmitter<String> outPurchase;
  @Output("outProduct")
  private OutputEmitter<String> outProduct;
  @Output("outCustomer")
  private OutputEmitter<String> outCustomer;

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    this.context = context;
    helper = new TransactionGeneratorHelper();
  }

  /**
   * Generates customers, products and purchase in accordance with the randomly generated "rules"
   *
   * @throws Exception
   */
  public void generate() throws Exception {
    // Generate a customer
    if (helper.getCustomers().size() < MAX_CUSTOMER) {
      outCustomer.emit(gson.toJson(helper.generateCustomer()));
    }

    // Generate a product
    if (helper.getProducts().size() < MAX_PRODUCT) {
      outProduct.emit(gson.toJson(helper.generateProduct()));
    }

    // Generate a purchase.
    if (helper.getPurchases().size() < MAX_PURCHASE) {
      outPurchase.emit(gson.toJson(helper.generatedPurchase()));
    }
  }
}

