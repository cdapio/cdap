package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

/**
 * Reads and routes all transactions types.
 */
public class StreamReaderFlowlet extends AbstractFlowlet {

  public enum TransactionType {
    Purchase,  /* 1 */
    Product,   /* 2 */
    Customer,   /* 3 */
    Unknown
  }

  private final Gson gson = new Gson();

  @Output("outPurchase")
  private OutputEmitter<Purchase> outPurchase;

  @Output("outProduct")
  private OutputEmitter<Product> outProduct;

  @Output("outCustomer")
  private OutputEmitter<Customer> outCustomer;

  public void process(StreamEvent event) {

    String body = new String(event.getBody().array());

    try {
      TransactionType transactionType = this.getTransactionType(body);
      body = this.preProcessJSON(body);

      switch (transactionType) {
        case Purchase:
          Purchase purchase = this.gson.fromJson(body, Purchase.class);
          outPurchase.emit(purchase);
          break;
        case Product:
          Product product = this.gson.fromJson(body, Product.class);
          outProduct.emit(product);
          break;
        case Customer:
          Customer customer = this.gson.fromJson(body, Customer.class);
          outCustomer.emit(customer);
          break;
        default:
          // ignore message, log error
      }
    } catch (JsonParseException jpe) {
      throw jpe;
    } finally {

    }
  }

  /**
   * Primitive transaction parser
   *
   * @param event
   * @return
   */
  private TransactionType getTransactionType(String event) {

    if (event.startsWith("1")) {
      return TransactionType.Purchase;
    } else if (event.startsWith("2")) {
      return TransactionType.Product;
    } else if (event.startsWith("3")) {
      return TransactionType.Customer;
    } else {
      return TransactionType.Unknown;
    }
  }

  /**
   * Pre process transactions, remove type argument
   * "{1,2,3}|{json_object}" return {json_object}
   *
   * @param event
   * @return
   */
  private String preProcessJSON(String event) {
    return event.substring(2);
  }
}
