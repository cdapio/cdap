/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.hdfs;

import co.cask.cdap.operations.OperationalStats;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;


/**
 * {@link OperationalStats} for HDFS nodes.
 */
@SuppressWarnings("unused")
public class HDFSNodes extends AbstractHDFSStats implements HDFSNodesMXBean {
  @VisibleForTesting
  static final String STAT_TYPE = "nodes";
  private static final Gson GSON = new Gson();

  private int namenodes;
  private int datanodes;

  public HDFSNodes() {
    this(new Configuration());
  }

  @VisibleForTesting
  HDFSNodes(Configuration conf) {
    super(conf);
  }

  @Override
  public String getStatType() {
    return "nodes";
  }

  @Override
  public int getNamenodes() {
    return namenodes;
  }

  @Override
  public int getDatanodes() {
    return datanodes;
  }

  @Override
  public void collect() throws IOException, JSONException {
    namenodes = getNameNodes().size();
    datanodes = getNumDataNodes();
  }

  private List<String> getNameNodes() throws IOException {
    List<String> namenodes = new ArrayList<>();
    if (!HAUtil.isHAEnabled(conf, getNameService())) {
      try (FileSystem fs = FileSystem.get(conf)) {
        return Collections.singletonList(fs.getUri().toString());
      }
    }
    String nameService = getNameService();
    for (String nnId : DFSUtil.getNameNodeIds(conf, nameService)) {
      namenodes.add(DFSUtil.getNamenodeServiceAddr(conf, nameService, nnId));
    }
    return namenodes;
  }

  private int getNumDataNodes() throws IOException, JSONException {
    int dataNodes = 0;

    URL url = new URL(getWebURL() + "/jmx");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);

    JSONObject responseJSON = new JSONObject(response.getResponseBodyAsString());
    JSONArray array = responseJSON.getJSONArray("beans");
    for (int idx = 0; idx < array.length(); ++idx) {
      if (array.getJSONObject(idx).get("name").equals("Hadoop:service=NameNode,name=FSNamesystemState")) {
        dataNodes = array.getJSONObject(idx).getInt("NumLiveDataNodes");
        break;
      }
    }

    return dataNodes;
  }

  @Nullable
  private String getNameService() {
    Collection<String> nameservices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
    if (nameservices.isEmpty()) {
      return null;
    }
    if (1 == nameservices.size()) {
      return Iterables.getOnlyElement(nameservices);
    }
    throw new IllegalStateException("Found multiple nameservices configured in HDFS. CDAP currently does not support " +
                                      "HDFS Federation.");
  }
}
