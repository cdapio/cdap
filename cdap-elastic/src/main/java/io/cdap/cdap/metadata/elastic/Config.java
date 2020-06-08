/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

/**
 * Configuration constants for the Elasticsearch metadata storage provider.
 */
public final class Config {
  private Config() { }

  static final String CONF_ELASTIC_HOSTS = "metadata.elasticsearch.cluster.hosts";
  static final String CONF_ELASTIC_TLS_VERIFY = "metadata.elasticsearch.tls.verify";
  static final String CONF_ELASTIC_USERNAME = "metadata.elasticsearch.credentials.username";
  static final String CONF_ELASTIC_PASSWORD = "metadata.elasticsearch.credentials.password";
  static final String CONF_ELASTIC_INDEX_NAME = "metadata.elasticsearch.index.name";
  static final String CONF_ELASTIC_SCROLL_TIMEOUT = "metadata.elasticsearch.scroll.timeout";
  static final String CONF_ELASTIC_NUM_SHARDS = "metadata.elasticsearch.num.shards";
  static final String CONF_ELASTIC_NUM_REPLICAS = "metadata.elasticsearch.num.replicas";
  static final String CONF_ELASTIC_WINDOW_SIZE = "metadata.elasticsearch.max.window.size";
  static final String CONF_ELASTIC_CONFLICT_NUM_RETRIES = "metadata.elasticsearch.conflict.num.retries";
  static final String CONF_ELASTIC_CONFLICT_RETRY_SLEEP_MS = "metadata.elasticsearch.conflict.retry.sleep.ms";

  static final String DEFAULT_ELASTIC_HOSTS = "localhost:9200";
  static final String DEFAULT_INDEX_NAME = "cdap.metadata";
  static final String DEFAULT_SCROLL_TIMEOUT = "60s";
  static final int DEFAULT_ELASTIC_CONFLICT_NUM_RETRIES = 50;
  static final int DEFAULT_ELASTIC_CONFLICT_RETRY_SLEEP_MS = 100;
  static final int DEFAULT_MAX_RESULT_WINDOW = 10000; // this is hardcoded in Elasticsearch
  static final boolean DEFAULT_ELASTIC_TLS_VERIFY = true;

  // index.mappings.json will have a mapping: "cdap_version": "CDAP_VERSION".
  // the latter (placeholder) is replaced with the current CDAP version at index creation
  // and similar for the metadata version and the checksum of the mappings file
  static final String MAPPING_CDAP_VERSION = "cdap_version";
  static final String MAPPING_CDAP_VERSION_PLACEHOLDER = "CDAP_VERSION";
  static final String MAPPING_METADATA_VERSION = "metadata_version";
  static final String MAPPING_METADATA_VERSION_PLACEHOLDER = "METADATA_VERSION";
  static final String MAPPING_MAPPING_CHECKSUM = "mapping_checksum";
  static final String MAPPING_MAPPING_CHECKSUM_PLACEHOLDER = "MAPPING_CHECKSUM";
}
