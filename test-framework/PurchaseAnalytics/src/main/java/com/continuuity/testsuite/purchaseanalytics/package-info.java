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
 *
 * This package contains a simple purchase analytics application. It illustrates how to use the ObjectStore dataset.
 *
 * Transactions types
 * -------------------
 * There are types of transactions; adding a product, customer and purchase. A purchase
 * must respect referential integrity and refer to and existing product and customer.
 * Each transaction json is prefixed by a transaction type, as described below:
 *
 * Purchase
 * 1|{"customer":"alex","product":"FisherPrice","quantity":10,"price":"100","purchaseTime":"129308132"}
 *
 * Product
 * 2|{"productId":"1","description":"FisherPrice"}
 *
 * Customer
 * 3|{"customerId":"1","name":"alex","zip":90210,"rating":100}
 *
 * Flows
 * -----
 * PurchaseAnalyticsFlow: Collects and stores transactions.
 * GeneratedPurchaseAnalyticsFlow: auto-generates and stores transactions.
 *
 * Batch Jobs
 * ----------
 * PurchaseHistoryBuilder: Aggregates all purchases made for each customer.
 * RegionBuilder: Aggregates all customers by zip code.
 * PurchaseStatsBuilder: Aggregates total and average spent for each customer.
 *
 * Procedure
 * ---------
 * PurchaseAnalyticsQuery
 * Method "history"
 * Returns purchase history for each customer
 * {"customer"}:{"customer_name"}
 *
 * Method "customer"
 * Returns information on a given customer
 * {"id":}:{customer_id}
 *
 * Method "region"
 * Returns number of customers per zip code.
 * {"zip"}:{zip_code}
 *
 * Method "addCustomer"
 * Adds a customer
 * {"customerId":"1","name":"alex","zip":90210,"rating":100}
 *
 * Method "addProduct"
 * Adds a product
 * {"productId":"1","description":"FisherPrice"}
 *
 * Method "add Purchase"
 * {"customer":"alex","product":"FisherPrice","quantity":10,"price":"100","purchaseTime":"timestamp"}
 *
 */


package com.continuuity.testsuite.purchaseanalytics;
