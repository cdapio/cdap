/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import java.util.List;

/**
 * The builder is such that from() must be called first. There can be any number of froms.
 * Then, to() can be called any non-zero number of times, and then build can be called.
 */
public class LineageConnection {

  private final List<Node> srcNodes;
  private final List<Node> dstNodes;

  private LineageConnection(List<Node> srcNodes, List<Node> dstNodes) {
    this.srcNodes = srcNodes;
    this.dstNodes = dstNodes;
  }

  public static FromBuilder builder() {
    return new FromToBuilder();
  }

  public interface FromBuilder {

    FromToBuilder from(String namespace, String dataset, String fieldName);

    FromToBuilder from(String fieldName);
  }

  public interface ToBuilder {

    ToBuilder to(String namespace, String dataset, String fieldName);

    ToBuilder to(String fieldName);

    LineageConnection build();
  }

  public static final class FromToBuilder implements FromBuilder, ToBuilder {

    List<Node> srcNodes;
    List<Node> dstNodes;

    @Override
    public FromToBuilder from(String namespace, String dataset, String fieldName) {
      srcNodes.add(new Node(namespace, dataset, fieldName));
      return this;
    }

    @Override
    public FromToBuilder from(String fieldName) {
      srcNodes.add(new Node(fieldName));
      return this;
    }

    @Override
    public ToBuilder to(String namespace, String dataset, String fieldName) {
      dstNodes.add(new Node(namespace, dataset, fieldName));
      return this;
    }

    @Override
    public ToBuilder to(String fieldName) {
      dstNodes.add(new Node(fieldName));
      return this;
    }

    @Override
    public LineageConnection build() {
      // TODO: copy lists and make immutable?
      return new LineageConnection(srcNodes, dstNodes);
    }
  }

  public static final class Node {
    final String namespace;
    final String dataset;
    final String fieldName;

    Node(String fieldName) {
      this(null, null, fieldName);
    }

    Node(String namespace, String dataset, String fieldName) {
      this.namespace = namespace;
      this.dataset = dataset;
      this.fieldName = fieldName;
    }
  }
}
