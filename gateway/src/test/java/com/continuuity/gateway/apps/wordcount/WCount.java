/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.gateway.apps.wordcount;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;

/**
 * Word count sample application.
 */
public class WCount implements Application {
  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WCount")
      .setDescription("another Word Count Application")
      .withStreams()
        .add(new Stream("words"))
      .withDataSets()
        .add(new Table("stats"))
        // note that "wordStats" is missing, hope our code deals with that!
        .add(new KeyValueTable("wordCounts"))
        .add(new UniqueCountTable("uniqueCount"))
        .add(new AssociationTable("wordAssocs"))
        .add(new KeyValueTable("jobConfig"))
      .withFlows()
        .add(new WCounter())
        .add(new WordCounter())
      .withProcedures()
        .add(new RCounts())
      .withMapReduce()
        .add(new ClassicWordCount())
      .noWorkflow()
      .build();
  }
}
