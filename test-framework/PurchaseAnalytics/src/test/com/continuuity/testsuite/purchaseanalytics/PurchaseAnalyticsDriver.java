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

import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;
import com.continuuity.testsuite.purchaseanalytics.datamodel.SerializedObject;
import com.google.gson.Gson;
import com.kenai.jaffl.annotations.In;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 *  Test driver for black box testing. can be run externally on any cluster.
 */
public class PurchaseAnalyticsDriver {
  private String hostname = "localhost"; // "goat170.dev.continuuity.net";
  private String streamName = "transactionStream";
  private TransactionGeneratorHelper helper;

  private static final int MAX_CUSTOMER = 1000;
  private static final int MAX_PRODUCT =  10000;
  private static final int MAX_PURCHASE = 100000;
  private static final long MAX_TRANSACTION = MAX_CUSTOMER + MAX_PRODUCT + MAX_PURCHASE;

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @Before
  public void initialize() {
    helper = new TransactionGeneratorHelper();
  }

  /**
   * Generates a specific set of transactions. verifies read and writes to datasets via procedure calls using the
   * REST API.
   */
  @Test
  public void blackBoxTestFlows() {

   // Generate a give set of transactions and publish to flow.
   this.generateTransactions();

   // Run Batch Jobs


  }

  /**
   * Generates customer, product and purchases.
   */
  private void generateTransactions() {
    try {

      Gson gson = new Gson();
      long numTransactions = 0;
      String json = "";

      while(numTransactions <= MAX_TRANSACTION) {
        if (helper.getCustomers().size() < MAX_CUSTOMER) {
          Customer customer = helper.generateCustomer();
          json = gson.toJson(customer);
          numTransactions++;
          this.streamTransaction(json, this.streamName);
        }

        if (helper.getProducts().size() < MAX_PRODUCT) {
          Product product = helper.generateProduct();
          json = gson.toJson(product);
          numTransactions++;
          this.streamTransaction(json, this.streamName);
        }

        if (helper.getPurchases().size() < MAX_PURCHASE) {
          Purchase purchase = helper.generatedPurchase();
          json = gson.toJson(purchase);
          numTransactions++;
          this.streamTransaction(json, this.streamName);
        }
      }
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Streams a transaction via REST API
   * eg: `curl -q -d "$product" http://localhost:10000/stream/transactionStream  #2>&1`
   *
   * @param json object to serialize
   * @throws IOException
   */
  private void streamTransaction(String json, String streamName) throws IOException {
    String url = "http://" + this.hostname + ":10000/stream/" + streamName;

    ProcessBuilder pb = new ProcessBuilder("curl", "-q", "-d", json, url);
    Process p = pb.start();

    InputStream is = p.getErrorStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    String line;
    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }
    p.destroy();
  }
}
