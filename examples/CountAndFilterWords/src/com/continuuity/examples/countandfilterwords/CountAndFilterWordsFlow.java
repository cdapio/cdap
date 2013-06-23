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

package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 *  CountAndFilterWords main Flow.
 */
public class CountAndFilterWordsFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountAndFilterWords")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("splitter", new Tokenizer())
        .add("upper-filter", new DynamicFilterFlowlet("upper", "^[A-Z]+$"))
        .add("lower-filter", new DynamicFilterFlowlet("lower", "^[a-z]+$"))
        .add("number-filter", new DynamicFilterFlowlet("number", "^[0-9]+$"))
        .add("count-all", new Counter())
        .add("count-upper", new Counter())
        .add("count-lower", new Counter())
        .add("count-number", new Counter())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("splitter")
        .from("splitter").to("count-all")
        .from("splitter").to("upper-filter")
        .from("splitter").to("lower-filter")
        .from("splitter").to("number-filter")
        .from("upper-filter").to("count-upper")
        .from("lower-filter").to("count-lower")
        .from("number-filter").to("count-number")
      .build();
  }
}
