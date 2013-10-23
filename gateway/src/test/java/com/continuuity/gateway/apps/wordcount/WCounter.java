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

package com.continuuity.gateway.apps.wordcount;


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
 * The first Flowlet is the WordSplitter which splits the sentence into
 * individual words, cleans up non-alpha characters, and then sends the
 * sentences on to the WordAssociater and the words on to the WordCounter.
 * <p>
 * The next Flowlet is the WordAssociater that will track word associations
 * between all of the words within the input string.
 * <p>
 * The next Flowlet is the WordCounter which performs the necessary data
 * operations to do the word count and count other word statistics.
 * <p>
 * The last Flowlet is the UniqueCounter which will calculate and update the
 * unique number of words seen.
 */
public class WCounter implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
        .setName("WCounter")
        .setDescription("Another Word Count Flow")
        .withFlowlets()
            .add("splitter", new WSplitter())
            .add("counter", new Counter())
            .add("associator", new WordAssociator())
            .add("unique", new UniqueCounter())
        .connect()
            .fromStream("words").to("splitter")
            .from("splitter").to("counter")
            .from("splitter").to("associator")
            .from("counter").to("unique")
        .build();
  }
}
