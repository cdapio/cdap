# Decryptor


Description
-----------
Decrypts one or more fields in input records using a keystore 
that must be present on all nodes of the cluster.


Configuration
-------------
**decryptFields** Specifies the fields to decrypt, separated by commas

**schema** Schema to pull records from

**transformation** Transformation algorithm, mode, and padding, separated by slashes; for example: AES/CBC/PKCS5Padding

**ivHex** The initialization vector if using CBC mode

**keystorePath** The path to the keystore on local disk; the keystore must be present on every node of the cluster

**keystorePassword** The password for the keystore

**keystoreType** The type of keystore; for example: JKS or JCEKS

**keyAlias** The alias of the key to use in the keystore

**keyPassword** The password for the key to use in the keystore

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
