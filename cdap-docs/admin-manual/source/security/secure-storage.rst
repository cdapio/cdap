.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-secure-storage:

==============
Secure Storage
==============

Applications can need controlled access to sensitive data such as passphrases, cryptographic keys, access tokens, and
passwords. This data is usually small in size, but needs to be stored and managed in a secure manner.
*Secure Storage* allows users to store such sensitive information in an secure and encrypted manner. Data is encrypted
upon submission to CDAP (via RESTful or programmatic APIs) and is decrypted upon retrieval.

**Note:** In CDAP 3.5.0, encryption and decryption of the contents only happens at the
secure store, not while the data is transitting to the secure store. In a later version of
CDAP, all transport involving secure keys will be secured using SSL.

.. _admin-secure-storage-format:

Secure Storage Format
---------------------
An entry in secure storage consists of:

- **Key**: An alias for the entry, also referred to as a :term:`Secure Key <secure key>`.
  Data is stored against the provided key and can be retrieved using the same key.
  Key must be of the :ref:`Alphanumeric Character Set <supported-characters>`, contain *only
  lowercase* characters, and should start with a letter.
- **Data**: The data which is to be stored in a secure and encrypted manner. This could be a passphrase,
  cryptographic key, access token, or any other data that needs to be stored securely.
- **Description**: A description for the secure store entry.
- **Properties**: A string map of properties for the secure storage entry. A ``creationTime`` property is available
  for all secure store entries by default. Additional properties can be supplied by users at the time of creation.

CDAP provides two different implementations of secure storage, depending on the runtime.

.. _admin-secure-storage-file:

File-backed Secure Storage
--------------------------
File-backed secure storage is available for use with in-memory (unit-test) and standalone (CDAP SDK) modes. It uses the
`Sun JCEKS <http://docs.oracle.com/javase/7/docs/technotes/guides/security/crypto/CryptoSpec.html#KeyManagement>`__
implementation for storing secure keys. This implementation is not available in
:term:`Distributed CDAP <distributed cdap>` as it stores the secure data in the local file system, and thus is
not available on all nodes of a distributed cluster.

To use this mode, set:

- ``security.store.provider`` to ``file`` in ``cdap-site.xml``; and
- ``security.store.file.password`` to a password (to protect the secure storage file) in ``cdap-security.xml``.

.. _admin-secure-storage-kms:

Hadoop Key Management Server (KMS)-backed Secure Storage
--------------------------------------------------------
`Hadoop KMS-backed <https://hadoop.apache.org/docs/stable/hadoop-kms/index.html>`__ secure storage is available for use
with :term:`Distributed CDAP <distributed cdap>`. To use this mode, set ``security.store.provider`` to ``kms``
in ``cdap-site.xml``.

For additional information on integration with Hadoop KMS, please refer to
:ref:`Integrations: Apache Hadoop KMS <apache-hadoop-kms>`.
