package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Abstract base class for {@link KeyManager} implementations that store all secret keys in an in-memory Map.
 */
public abstract class MapBackedKeyManager extends AbstractKeyManager {
  protected final Map<Integer, KeyIdentifier> allKeys = Maps.newConcurrentMap();

  public MapBackedKeyManager(CConfiguration cConf) {
    super(cConf);
  }

  @Override
  protected boolean hasKey(int id) {
    return allKeys.containsKey(id);
  }

  @Override
  protected KeyIdentifier getKey(int id) {
    return allKeys.get(id);
  }

  @Override
  protected void addKey(KeyIdentifier key) {
    allKeys.put(key.getKeyId(), key);
  }
}
