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

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;
import com.continuuity.testsuite.purchaseanalytics.datamodel.PurchaseHistory;

/**
 * A procedure that allows to query a customer's purchase history.
 */
public class PurchaseAnalyticsQuery extends AbstractProcedure {

  @UseDataSet("history")
  private ObjectStore<PurchaseHistory> purchaseHistory;
  @UseDataSet("customers")
  private ObjectStore<Customer> customers;
  @UseDataSet("region")
  private KeyValueTable regions;
  @UseDataSet("products")
  private ObjectStore<Product> products;
  @UseDataSet("purchases")
  private ObjectStore<Purchase> purchases;

  /**
   * Given a customer name, return his purchases as a Json .
   *
   * @param request the request, must contain an argument named "customer"
   */
  @Handle("history")
  //@SuppressWarnings("unused")
  public void history(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String customer = request.getArgument("customer");
    PurchaseHistory history = purchaseHistory.read(customer.getBytes());
    if (history == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No purchase history for " + customer);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), history);
    }
  }

  @Handle("customer")
  public void customer(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String id = request.getArgument("id");
    Customer customer = customers.read(Bytes.toBytes(Long.parseLong(id)));

    if (customer == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No customer for id " + id);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), customer);
    }
  }

  @Handle("product")
  public void product(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String id = request.getArgument("id");
    Product product = products.read(Bytes.toBytes(Long.parseLong(id)));

    if (product == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No customer for id " + id);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), product);
    }
  }

  @Handle("purchase")
  public void purchase(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String id = request.getArgument("time");
    Purchase purchase = purchases.read(Bytes.toBytes(Long.parseLong(id)));

    if (purchase == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No customer for id " + id);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), purchase);
    }
  }

  @Handle("region")
  public void region(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String zip = request.getArgument("zip");


    byte[] numCustomers = regions.read(Bytes.toBytes(Integer.parseInt(zip)));

    if (numCustomers == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, "No value found for zipcode " + zip);
    } else {
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), Bytes.toInt(numCustomers));
    }
  }

  /**
   * Procedure to test dataset insertion, in this case, object store.
   *
   * @param request
   * @param responder
   * @throws Exception
   */
  @Handle("addCustomer")
  public void addCustomer(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String customerId = request.getArgument("customerId");
    String name = request.getArgument("name");
    String zip = request.getArgument("zip");
    String rating = request.getArgument("rating");

    if (customerId == null || name == null || zip == null || rating == null) {
      responder.error(ProcedureResponse.Code.FAILURE, "Unable to process customer addition.");
    } else {
      // Insert in dataset (long customerId, String name, int zip, int rating)
      Customer customer = new Customer(Long.parseLong(customerId),
                                       name, Integer.parseInt(zip), Integer.parseInt(rating));

      customers.write(Bytes.toBytes(customerId), customer);
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), "Customer added to system.");
    }
  }

  @Handle("addProduct")
  public void addProduct(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String productId = request.getArgument("productId");
    String description = request.getArgument("description");

    if (productId == null || description == null) {
      responder.error(ProcedureResponse.Code.FAILURE, "Unable to process product addition.");
    } else {
      Product product = new Product(Long.parseLong(productId), description);
      products.write(Bytes.toBytes(Long.parseLong(productId)), product);
    }
  }

  @Handle("addPurchase")
  public void addPruchase(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    String productId = request.getArgument("productid");
    String customerId = request.getArgument("customerid");
    String quantity = request.getArgument("quantity");
    String price = request.getArgument("price");

    if (productId == null || customerId == null || quantity == null) {
      responder.error(ProcedureResponse.Code.FAILURE, "Unable to create purchase. Missing parameters.");
    } else {

      // get product and customer to create a purchase.
      Product product = products.read(Bytes.toBytes(productId));
      Customer customer = customers.read(Bytes.toBytes(customerId));

      if (product == null || customer == null) {
        responder.error(ProcedureResponse.Code.FAILURE, "Unable to retrieve specified product or customer.");
      } else {
        // (String customer, String product, int quantity, int price, long purchaseTime)
        Purchase purchase = new Purchase(customer.getName(),
                                         product.getDescription(),
                                         Integer.parseInt(quantity),
                                         Integer.parseInt(price),
                                         System.currentTimeMillis());

        purchases.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
      }
    }
  }
}
