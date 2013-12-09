package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Product;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 *
 */
public class TransactionGeneratorHelper {
  private static final int latency = 10;
  private long currentCustomerId = 0;
  private long currentProductId = 0;

  private ArrayList<Customer> customers;
  private ArrayList<Product> products;
  private ArrayList<Purchase> purchases;

  public TransactionGeneratorHelper() {
    customers = new ArrayList<Customer>();
    products = new ArrayList<Product>();
    purchases = new ArrayList<Purchase>();
  }

  public long getCurrentCustomerId() {
    return currentCustomerId;
  }

  public long getCurrentProductId() {
    return currentProductId;
  }

  public ArrayList<Customer> getCustomers() {
    return customers;
  }

  public ArrayList<Product> getProducts() {
    return products;
  }

  public ArrayList<Purchase> getPurchases() {
    return purchases;
  }

  /**
   * Generates a random customer and stores in customer list for correlation.
   *
   * @return randomized customer
   */
  public Customer generateCustomer() {
    Customer customer = new Customer(this.currentCustomerId++,                 /* Incremental unique Id */
                                     UUID.randomUUID().toString(), /* Unique random name */
                                     this.randInt(10000, 99999),   /* zipcode semi-valid */
                                     this.randInt(1, 100));        /* Customer rating 1-100 */
    customers.add(customer);
    return customer;
  }

  public Product generateProduct() {
    Product product = new Product(this.currentProductId++, UUID.randomUUID().toString());
    products.add(product);
    return product;
  }

  /**
   * Randomly selects a customer and a product to create a purchase.
   *
   * @return
   */
  public Purchase generatedPurchase() {
    Customer customer = this.customers.get(this.randInt(0, this.customers.size() - 1));
    Product product = this.products.get(this.randInt(0, this.products.size() - 1));

    Purchase purchase = new Purchase(customer.getName(),
                                     product.getDescription(),
                                     this.randInt(1, 100),
                                     this.randInt(1, 999999),
                                     System.currentTimeMillis());
    this.purchases.add(purchase);
    return purchase;
  }

  public int randInt(int min, int max) {
    Random rand = new Random();
    return rand.nextInt((max - min) + 1) + min;
  }
}

