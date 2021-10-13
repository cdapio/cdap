/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.preview;

import java.util.ArrayList;
import java.util.List;

public class Metrics {
  String kind = "MetricValueList";
  String apiVersion = "custom.metrics.k8s.io/v1beta1";
  Metadata metadata = new Metadata();
  List<Item> items;

  public Metrics(String value) {
    Item item = new Item();
    item.value = value;
    //TODO
    item.timestamp = "";
    items = new ArrayList<>();
    items.add(item);
  }

  public static class Item {
    DescribedObject describedObject = new DescribedObject();
    String metricName = "qlen";
    String timestamp;
    String value;
  }

  public static class DescribedObject {
    String kind = "Service";
    String namespace = "default";
    String name = "cdap-autoscale-preview";
    String apiVersion = "/v1beta1";
  }

  public static class Metadata {
    String selfLink = "/apis/custom.metrics.k8s.io/v1beta1";
  }
}
