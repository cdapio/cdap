package com.continuuity.examples.purchase;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;

/**
 *
 */
public class PurchaseApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with().
        setName("PurchaseHistory").
        setDescription("illustrates the use of object store for tracking purchases").
        withStreams().add(new Stream("purchases")).
        withDataSets().add(new ObjectStore<ArrayList<Purchase>>(
          "purchases", new TypeToken<ArrayList<Purchase>>() { }.getType())).
        withFlows().add(new PurchaseFlow()).
        withProcedures().add(new PurchaseQuery()).
        build();
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException(e);
    }
  }
}
