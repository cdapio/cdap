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

package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * The SimpleWriteAndRead application uses one dataset, one flow, three flowlets, and one stream to demonstrate
 * how to write to and read from a dataset. 
 */
public class SimpleWriteAndRead implements Application {

  public static final String TABLE_NAME = "writeAndRead";

  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("Flow that writes key=value then reads back the key")
      .withStreams()
        .add(new Stream("keyValues"))
      .withDataSets()
        .add(new KeyValueTable(TABLE_NAME))
      .withFlows()
        .add(new SimpleWriteAndReadFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
