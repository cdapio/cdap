/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.collect.Lists;

import java.net.URI;
import java.util.List;

/**
 * This is a sample Web Crawler application that is used is test.
 * <p>
 *   This Application has multiple flowlets
 *   <ul>
 *     <li>Document Crawler : Gets urls from stream and crawls the pages and stores them. </li>
 *   </ul>
 * </p>
 */
public class ToyApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ToyFlowApp")
      .setDescription("Toy Flow Application")
      .withStreams().add(new Stream("X")).add(new Stream("Y"))
      .withDataSets().add(new KeyValueTable("crawled-pages"))
      .withFlows().add(new ToyFlow())
      .noProcedure().build();
  }

  public static final class ToyFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("ToyFlow")
        .setDescription("Complex Toy Flow")
        .withFlowlets().add(new A()).apply()
        .add(new B()).apply()
        .add(new C()).apply()
        .add(new D()).apply()
        .add(new E()).apply()
        .add(new F()).apply()
        .add(new G()).apply()
        .connect().from(new Stream("X")).to(new A())
        .from(new Stream("Y")).to(new B())
        .from(new A()).to(new C())
        .from(new B()).to(new E())
        .from(new A()).to(new E())
        .from(new C()).to(new D())
        .from(new C()).to(new F())
        .from(new D()).to(new G())
        .from(new F()).to(new G())
        .from(new E()).to(new G())
        .build();
    }
  }

  public static final class A extends AbstractFlowlet {
    private OutputEmitter<String> out;
    @Output("out1")
    private OutputEmitter<Float> out1;

    public void process(StreamEvent event) {
      out.emit("out");
      out1.emit(2.3f);
    }
  }

  public static final class B extends AbstractFlowlet {
    private OutputEmitter<Boolean> out;

    public void process(StreamEvent event) {
      out.emit(false);
    }
  }

  public static final class C extends AbstractFlowlet {
    @Output("c1")
    private OutputEmitter<Long> c1;

    @Output("c2")
    private OutputEmitter<Integer> c2;

    public void process(String A) {
      c1.emit(1L);
      c2.emit(1);
    }
  }

  public static final class E extends AbstractFlowlet {
    private OutputEmitter<Double> out;

    @Process("out1")
    void process(Float f) {
      out.emit(1.2);
    }

    void process(Boolean b) {
      out.emit(1.5);
    }
  }

  public static final class D extends AbstractFlowlet {
    @Output("d1")
    private OutputEmitter<List<String>> out;

    @Process("c1")
    void process(Long l) {
      List<String> p = Lists.newArrayList();
      out.emit(p);
    }
  }

  public static final class F extends AbstractFlowlet {
    @Output("f1")
    private OutputEmitter<URI> f1;

    @Process("c2")
    void process(Integer i) {
      f1.emit(URI.create("http://www.google.com"));
    }
  }

  public static final class G extends AbstractFlowlet {
    @Process("d1")
    public void process(List<String> s) {

    }

    @Process("f1")
    public void process(URI uri) {

    }

    public void process(Double d) {

    }
  }


}
