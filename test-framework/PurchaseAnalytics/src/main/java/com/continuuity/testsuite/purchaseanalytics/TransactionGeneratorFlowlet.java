package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * Generates Purchases, Products, customers and inventory
 */
public class TransactionGeneratorFlowlet extends AbstractGeneratorFlowlet {
  private static final int productRate = 3;
  private static final int purchaseRate = 5;
  private static final int generationLatency = 10;
  private static final long stopLimitProducts = 10000;
  private static final long stopLimitCustomers = stopLimitProducts * 100;
  private static long customerId = 0;
  private static long productId = 0;

  private FlowletContext context;

  private ArrayList<Customer> customers;
  private ArrayList<Product> products;

  @Output("outPurchase")
  private OutputEmitter<Purchase> outPurchase;
  @Output("outProduct")
  private OutputEmitter<Product> outProduct;
  @Output("outCustomer")
  private OutputEmitter<Customer> outCustomer;

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    this.context = context;
    customers = new ArrayList<Customer>();
    products = new ArrayList<Product>();
  }

  /**
   * Generates customers, products and purchase in accordance with the randomly generated "inventory"
   *
   * @throws Exception
   */
  public void generate() throws Exception {
    // Generate a customer
    if (customers.size() < stopLimitCustomers) {
      outCustomer.emit(this.generateCustomer());
    }

    if (products.size() < stopLimitProducts) {
      // Generate products
      for (int i = 0; i <= productRate; i++) {
        outProduct.emit(this.generateProduct());
      }
    }

    // Generate purchases indefinitely.
    for (int i = 0; i <= purchaseRate; i++) {
      outPurchase.emit(this.generatedPurchase());
    }

    Thread.sleep(generationLatency);
  }

  /**
   * Generates a random customer and stores in customer list for correlation.
   *
   * @return randomized customer
   */
  private Customer generateCustomer() {
    Customer customer = new Customer(customerId++,                 /* Incremental unique Id */
                                     UUID.randomUUID().toString(), /* Unique random name */
                                     this.randInt(10000, 99999),   /* zipcode semi-valid */
                                     this.randInt(1, 100));        /* Customer rating 1-100 */
    customers.add(customer);
    return customer;
  }

  private Product generateProduct() {
    Product product = new Product(productId++, UUID.randomUUID().toString());
    products.add(product);
    return product;
  }

  /**
   * Randomly selects a customer and a product to create a purchase.
   *
   * @return
   */
  private Purchase generatedPurchase() {
    Customer customer = this.customers.get(this.randInt(0, this.customers.size() - 1));
    Product product = this.products.get(this.randInt(0, this.products.size() - 1));

    Purchase purchase = new Purchase(customer.getName(),
                                     product.getDescription(),
                                     this.randInt(1, 100),
                                     this.randInt(1, 999999),
                                     System.currentTimeMillis());
    return purchase;
  }

  private int randInt(int min, int max) {
    Random rand = new Random();
    return rand.nextInt((max - min) + 1) + min;
  }
}

