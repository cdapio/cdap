/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.wordcount;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Flow that takes any arbitrary string of input and performs word statistics.
 * <p>
 * Flow parses input string into individual words, then performs per-word counts
 * and other calculations like total number of words seen, average length
 * of words seen, unique words seen, and also tracks the words most often
 * associated with each other word.
 * <p>
 * The first Flowlet is the WordSplitter, which splits the sentence into
 * individual words, cleans up non-alpha characters, and then sends the
 * sentences to the WordAssociater and the words on to the WordCounter.
 * <p>
 * The next Flowlet is the WordAssociater that will track word associations
 * between all of the words within the input string.
 * <p>
 * The next Flowlet is the WordCounter, which performs the necessary data
 * operations to do the word count and count other word statistics.
 * <p>
 * The last Flowlet is the UniqueCounter, which calculates and updates the
 * unique number of words seen.
 */
public class WordCounter implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
        .setName("WordCounter")
        .setDescription("Example Word Count Flow")
        .withFlowlets()
            .add("splitter", new WordSplitter())
            .add("associator", new WordAssociator())
            .add("counter", new Counter())
            .add("unique", new UniqueCounter())
        .connect()
            .fromStream("wordStream").to("splitter")
            .from("splitter").to("associator")
            .from("splitter").to("counter")
            .from("counter").to("unique")
        .build();
  }
}
