package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;

import java.util.List;

public interface SecureStoreService {
  List<SecureKeyListEntry> list(NamespaceId namespaceId) throws Exception;

  SecureStoreData get(SecureKeyId secureKeyId) throws Exception;

  void put(SecureKeyCreateRequest secureKeyCreateRequest, SecureKeyId secureKeyId)
    throws Exception;

  void delete(SecureKeyId secureKeyId) throws Exception;
}
