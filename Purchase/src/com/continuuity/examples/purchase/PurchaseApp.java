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

package com.continuuity.examples.purchase;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;

/**
 * Simple purchase app that demonstrate how to use object store and map reduce batch for tracking purchases.
 */
public class PurchaseApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("PurchaseHistory")
        .setDescription("illustrates the use of object store for tracking purchases")
        .withStreams().add(new Stream("purchases"))
        .withDataSets().add(new ObjectStore<ArrayList<Purchase>>(
          "purchases", new TypeToken<ArrayList<Purchase>>() { }.getType()))
        .withFlows()
          .add(new PurchaseFlow())
        .withProcedures()
          .add(new PurchaseQuery())
        .noBatch()
        .build();
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException(e);
    }
  }
}
