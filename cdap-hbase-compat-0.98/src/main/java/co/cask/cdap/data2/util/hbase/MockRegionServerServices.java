/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class MockRegionServerServices implements RegionServerServices {
  private final Configuration hConf;
  private final ZooKeeperWatcher zookeeper;
  private final Map<String, HRegion> regions = new HashMap<String, HRegion>();
  private boolean stopping = false;
  private final ConcurrentSkipListMap<byte[], Boolean> rit =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);
  private HFileSystem hfs = null;
  private ServerName serverName = null;
  private RpcServerInterface rpcServer = null;
  private volatile boolean abortRequested;


  public MockRegionServerServices(Configuration hConf, ZooKeeperWatcher zookeeper) {
    this.hConf = hConf;
    this.zookeeper = zookeeper;
  }

  @Override
  public boolean isStopping() {
    return stopping;
  }

  @Override
  public HLog getWAL(HRegionInfo regionInfo) throws IOException {
    return null;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return null;
  }

  @Override
  public FlushRequester getFlushRequester() {
    return null;
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return null;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return new TableLockManager.NullTableLockManager();
  }

  @Override
  public void postOpenDeployTasks(HRegion r, CatalogTracker ct) throws KeeperException, IOException {
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return rit;
  }

  @Override
  public FileSystem getFileSystem() {
    return hfs;
  }

  @Override
  public Leases getLeases() {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return null;
  }

  @Override
  public Map<String, HRegion> getRecoveringRegions() {
    return null;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName, List<HBaseProtos.ServerName> serverNames) {

  }

  @Override
  public List<HRegion> getOnlineRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return new InetSocketAddress[0];
  }

  @Override
  public void addToOnlineRegions(HRegion r) {
    regions.put(r.getRegionNameAsString(), r);
  }

  @Override
  public boolean removeFromOnlineRegions(HRegion r, ServerName destination) {
    return regions.remove(r.getRegionInfo().getEncodedName()) != null;
  }

  @Override
  public HRegion getFromOnlineRegions(String encodedRegionName) {
    return regions.get(encodedRegionName);
  }

  @Override
  public Configuration getConfiguration() {
    return hConf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zookeeper;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public void abort(String why, Throwable e) {
    this.abortRequested = true;
  }

  @Override
  public boolean isAborted() {
    return abortRequested;
  }

  @Override
  public void stop(String why) {
    this.stopping = true;
  }

  @Override
  public boolean isStopped() {
    return stopping;
  }

  @Override
  public boolean reportRegionTransition(RegionServerStatusProtos.RegionTransition.TransitionCode transitionCode,
                                        long l, HRegionInfo... hRegionInfos) {
    return false;
  }

  @Override
  public boolean reportRegionTransition(RegionServerStatusProtos.RegionTransition.TransitionCode transitionCode,
                                        HRegionInfo... hRegionInfos) {
    return false;
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return null;
  }

  @Override
  public int getPriority(RPCProtos.RequestHeader requestHeader, Message message) {
    return 0;
  }
}
