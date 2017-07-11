# Encryptor


Description
-----------
Encrypts one or more fields in input records using a java keystore 
that must be present on all nodes of the cluster.


Configuration
-------------
**encyrptFields** Specifies the fields to encrypt, separated by commas.

**transformation** Transformation algorithm/mode/padding. For example, AES/CBC/PKCS5Padding.

**ivHex** The initialization vector if using CBC mode.

**keystorePath** The path to the keystore on local disk. The keystore must be present on every node of the cluster.

**keystorePassword** The password for the keystore.

**keystoreType** The type of keystore. For example, JKS, or JCEKS.

**keyAlias** The alias of the key to use in the keystore.

**keyPassword** The password for the key to use in the keystore.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
