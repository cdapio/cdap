package com.continuuity.examples.purchase;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.util.List;

/**
 * This implements a simple purchase history application. See the package info for more details.
 */
public class PurchaseApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("PurchaseHistory")
        .setDescription("Purchase history application")
        .withStreams()
          .add(new Stream("purchaseStream"))
        .withDataSets()
          .add(new ObjectStore<PurchaseHistory>("history", PurchaseHistory.class))
          .add(new ObjectStore<List<Purchase>>("purchases", Purchase.class))
        .withFlows()
          .add(new PurchaseFlow())
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
