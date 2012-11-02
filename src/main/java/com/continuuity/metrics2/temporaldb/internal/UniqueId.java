package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.metrics2.temporaldb.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
class UniqueId {
  private static final Logger Log = LoggerFactory.getLogger(UniqueId.class);
  private final KVStore store;
  private final String prefix;
  private final Map<String, Integer> cache = new ConcurrentHashMap<String, Integer>();
  private final Map<Integer, String> invertedCache = new ConcurrentHashMap<Integer, String>();
  private final byte[] MAX_ID_KEY;
  private final Charset charset = Charset.forName("utf-16");
  private final int ID_START = 0;

  public UniqueId(KVStore store, String prefix) {
    this.store = store;
    this.prefix = prefix;
    MAX_ID_KEY = ("current_maxid_" + prefix).getBytes(charset);
  }

  public synchronized int getOrCreateId(String name) throws Exception {
    if(name == null || name.isEmpty()) {
      throw new Exception("Metric name is empty or null.");
    }
    Integer cached = cache.get(name);
    if (cached != null) {
      return cached;
    }
    int currentID = getMaxID();
    int nextID = currentID + 1;
    byte[] key = createKey(nextID);
    store.put(key, name.getBytes(charset));
    doCache(nextID, name);
    setMaxID(nextID);
    return nextID;
  }

  public int getMaxID() throws Exception {
    byte[] maxId = store.get(MAX_ID_KEY);
    if (maxId == null) {
      Log.trace("_ maxid=" + ID_START);
      return ID_START;
    } else if (maxId.length == 4) {
      int maxIdAsInt = Bytes.getInt(maxId);
      Log.trace("< maxid=" + maxIdAsInt + " [" + Bytes.toHex(maxId)
                    + "]");
      return maxIdAsInt;
    } else {
      throw new IllegalStateException("invalid current_maxid=" + maxId);
    }
  }

  public void setMaxID(int maxID) throws Exception {
    byte[] maxIdAsBytes = Bytes.fromInt(maxID);
    store.put(MAX_ID_KEY, maxIdAsBytes);
    Log.trace("> maxid=" + maxID + " [" + Bytes.toHex(maxIdAsBytes)
                  + "] [" + Bytes.toHex(MAX_ID_KEY) + "]");
  }

  private byte[] createKey(int id) {
    String key = prefix + "_" + id;
    return key.getBytes(charset);
  }

  public String getValue(int id) throws Exception {
    String cached = invertedCache.get(id);
    if (cached != null) {
      return cached;
    }
    byte[] key = createKey(id);
    byte[] fromstore = store.get(key);
    if (fromstore != null) {
      String name = new String(fromstore, charset);
      doCache(id, name);
      return name;
    }

    for (Entry<String, Integer> entry : cache.entrySet()) {
      if (entry.getValue() != null && entry.getValue() == id) {
        return entry.getKey();
      }
    }
    return null;
  }

  private void doCache(int id, String name) {
    cache.put(name, id);
    invertedCache.put(id, name);
  }

  public void init() throws Exception {
    int maxID = getMaxID();
    if (maxID == ID_START) {
      return;
    }

    for (int i = ID_START; i < maxID; i++) {
      int id = i + 1;
      // the only purpose is to fill the cache
      String val = getValue(id);
      Log.trace("Read '" + val + "' from cache using id '" + id + "'");
    }
  }
}
