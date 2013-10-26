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

package com.continuuity.examples.ticker.data;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.procedure.AbstractProcedure;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 *
 */
public class AppWithMultiIndexedTable implements Application {

  @Override
  public ApplicationSpecification configure() {
    Set<byte[]> ignoredFields = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
    return ApplicationSpecification.Builder.with()
      .setName("AppWithMultiIndexedTable")
      .setDescription("Simple app with indexed table dataset")
      .noStream()
      .withDataSets().add(new MultiIndexedTable("indexedTable", Bytes.toBytes("ts"), ignoredFields))
      .noFlow()
      .withProcedures().add(new AbstractProcedure("fooProcedure") {
      })
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
