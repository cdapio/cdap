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

package com.continuuity.testsuite.purchaseanalytics.appdriver;

import com.continuuity.testsuite.purchaseanalytics.TransactionGeneratorHelper;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;
import com.continuuity.testsuite.purchaseanalytics.datamodel.SerializedObject;
import com.google.gson.Gson;
import org.mortbay.log.Log;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Test driver for black box testing. can be run externally on any cluster.
 */
public class PurchaseAnalyticsAppDriver {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PurchaseAnalyticsAppDriver.class);
  private String hostname;
  private String streamName = "transactionStream";
  private TransactionGeneratorHelper helper;
  private long numTransactions;
  private static final Gson gson = new Gson();

  public PurchaseAnalyticsAppDriver(String hostname, long numTransactions) {
    this.numTransactions = numTransactions;
    this.hostname = hostname;
    helper = new TransactionGeneratorHelper();
  }

  public static void main(String[] args) {
    if (args == null || args.length < 2) {
      System.out.print("Usage: PurchaseAnalyticsAppDriver [hostname] [number of transactions]");
    }

    String hostname = args[0];
    long transactions = 0;

    try {
      transactions = Long.parseLong(args[1]);
    } catch (NumberFormatException ex) {
      LOG.debug(ex.getMessage());
    }

    PurchaseAnalyticsAppDriver appDriver = new PurchaseAnalyticsAppDriver(hostname, transactions);

    try {
      appDriver.startComponent("PurchaseAnalyticsFlow", "flow");
      appDriver.generateTransactions();


      Customer returnedCustomer = null;
      // Verify that all customers streamed have been inserted
      for (Customer customer : appDriver.helper.getCustomers()) {

        if (!appDriver.customerExists(customer)) {
           LOG.error("Customer not found: " + gson.toJson(customer));
        }
      }

      for (Product product : appDriver.helper.getProducts()) {
        if (!appDriver.productExists(product)) {
          LOG.error("Customer not found: " + gson.toJson(product));
        }

      }

      // Run map reduce jobs.
      appDriver.startComponent("PurchaseHistoryBuilder", "mapreduce");
      appDriver.startComponent("PurchaseStatsBuilder", "mapreduce");
      appDriver.startComponent("RegionBuilder", "mapreduce");

      // TODO: Wait for status on MR jobs, correlate result from MR job


    } catch (IOException ex) {

    }


    // Verify results.


  }

  /**
   * Generates customer, product and purchases.
   */
  private void generateTransactions() {
    try {
      long transactions = 0;
      String json = "";

      while (transactions <= this.numTransactions) {
        Customer customer = helper.generateCustomer();
        json = gson.toJson(customer);
        transactions++;
        this.streamTransaction(json, this.streamName);

        Product product = helper.generateProduct();
        json = gson.toJson(product);
        transactions++;
        this.streamTransaction(json, this.streamName);

        Purchase purchase = helper.generatedPurchase();
        json = gson.toJson(purchase);
        transactions++;
        this.streamTransaction(json, this.streamName);
      }
    } catch (IOException ex) {
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
      LOG.debug(line);
    }
    p.destroy();
  }

  /**
   * REST starts a component {flow, procedure, mapreduce}
   *
   * @param name
   * @param type
   * @throws IOException
   */
  private void startComponent(String name, String type) throws IOException {
    String url = "http://" + this.hostname + ":10007/" + type + "/PurchaseAnalytics/" + name;

    ProcessBuilder pb = new ProcessBuilder("curl", "-s", "-X", "POST", url);
    Process p = pb.start();

    InputStream is = p.getErrorStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    String line;
    while ((line = br.readLine()) != null) {
      LOG.debug(line);
    }
    p.destroy();
  }

  /**
   * Gets a customer by Id
   *
   * @param customer
   * @return
   */
  private boolean customerExists(Customer customer) throws IOException {
    String url = "http://" + this.hostname + ":10010/procedure/PurchaseAnalytics/PurchaseAnalyticsQuery/customer";
    String json = "{\"id\":" + customer.getCustomerId() + "}";

    ProcessBuilder pb = new ProcessBuilder("curl", "-s", "-X", "POST", url, "--data", json);
    Process p = pb.start();
    InputStream is = p.getErrorStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    String line = br.readLine();
    Gson gson = new Gson();
    Customer returnedCustomer = gson.fromJson(line, Customer.class);

    p.destroy();

    return (returnedCustomer == customer);
  }

  private boolean productExists(Product product) throws IOException {
    String url = "http://" + this.hostname + ":10010/procedure/PurchaseAnalytics/PurchaseAnalyticsQuery/product";
    String json = "{\"id\":" + product.getProductId() + "}";

    ProcessBuilder pb = new ProcessBuilder("curl", "-s", "-X", "POST", url, "--data", json);
    Process p = pb.start();
    InputStream is = p.getErrorStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    String line = br.readLine();
    Gson gson = new Gson();
    Product returnedProduct = gson.fromJson(line, Product.class);

    p.destroy();

    return (returnedProduct == product);
  }

  private boolean purchaseExists(Purchase purchase) throws IOException {
    String url = "http://" + this.hostname + ":10010/procedure/PurchaseAnalytics/PurchaseAnalyticsQuery/purchase";
    String json = "{\"time\":" + purchase.getPurchaseTime() + "}";

    ProcessBuilder pb = new ProcessBuilder("curl", "-s", "-X", "POST", url, "--data", json);
    Process p = pb.start();
    InputStream is = p.getErrorStream();
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    String line = br.readLine();
    Gson gson = new Gson();
    Purchase returnedPurchase = gson.fromJson(line, Purchase.class);

    p.destroy();

    return (returnedPurchase == purchase);
  }
}
