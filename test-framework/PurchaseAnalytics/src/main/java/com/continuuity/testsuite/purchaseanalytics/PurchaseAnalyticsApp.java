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

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.util.List;

/**
 *
 * This implements a simple purchase history application. See the package info for more details.
 */
public class PurchaseAnalyticsApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("PurchaseAnalytics")
        .setDescription("Purchase Analytics App")
        .withStreams()
          .add(new Stream("trx"))
        .withDataSets()
          .add(new ObjectStore<PurchaseHistory>("history", PurchaseHistory.class))
          .add(new ObjectStore<Purchase>("purchases", Purchase.class))
          .add(new ObjectStore<Product>("products", Product.class))
          .add(new ObjectStore<Inventory>("inventory", Inventory.class))
          .add(new ObjectStore<Customer>("customers", Customer.class))
        .withFlows()
          .add(new PurchaseAnalyticsFlow())
        .withProcedures()
          .add(new PurchaseQuery())
        .withBatch()
          .add(new PurchaseHistoryBuilder())
        .build();
    } catch (UnsupportedTypeException e) {
      // this exception is thrown by ObjectStore if its parameter type cannot be (de)serialized (for example, if it is
      // an interface and not a class, then there is no auto-magic way deserialize an object. In this case that
      // cannot happen because PurchaseHistory is an actual class.
      throw new RuntimeException(e);
    }
  }
}
