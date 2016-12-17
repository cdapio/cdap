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
File-backed secure storage is available for use with in-memory CDAP (unit-test) and
standalone CDAP (as included in the CDAP SDK) modes. It uses the
`Sun JCEKS <http://docs.oracle.com/javase/7/docs/technotes/guides/security/crypto/CryptoSpec.html#KeyManagement>`__
implementation for storing secure keys. This implementation is not available in
:term:`Distributed CDAP <distributed cdap>` as it stores the secure data in the local file system, and thus is
not available on all nodes of a distributed cluster.

To use this mode, set these properties:

- In ``cdap-site.xml``, set ``security.store.provider`` to ``file``::

    <property>
      <name>security.store.provider</name>
      <value>file</value>
      <description>
        Backend provider for the secure store
      </description>
    </property>

- In ``cdap-security.xml``, set ``security.store.file.password`` to a password (to protect the secure storage file).
  **Note:** If the ``cdap-security.xml`` file does not already exist, it needs to be created::

    <property>
      <name>security.store.file.password</name>
      <value>your password</value>
      <description>
        Password to access the key store
      </description>
    </property>
    
- The path and the filename of the backing file can be configured in ``cdap-site.xml``
  using these (optional) settings::

    <property>
      <name>security.store.file.path</name>
      <value>${local.data.dir}/store</value>
      <description>
        Location of the encrypted file which holds the secure store entries
      </description>
    </property>
  
    <property>
      <name>security.store.file.name</name>
      <value>securestore</value>
      <description>
        Name of the secure store file
      </description>
    </property>

.. _admin-secure-storage-kms:

Hadoop Key Management Server-backed Secure Storage
--------------------------------------------------
`Hadoop KMS (Key Management Server)-backed <https://hadoop.apache.org/docs/stable/hadoop-kms/index.html>`__
secure storage is available for use with :term:`Distributed CDAP <distributed cdap>`.

To use this mode, set this property:

- In ``cdap-site.xml``, set ``security.store.provider`` to ``kms``::

    <property>
      <name>security.store.provider</name>
      <value>kms</value>
      <description>
        Backend provider for the secure store
      </description>
    </property>

For additional information on integration with Hadoop KMS, please refer to
:ref:`Integrations: Apache Hadoop KMS <apache-hadoop-kms>`.

Accessing the Secure Storage
----------------------------
The :ref:`Secure Storage HTTP RESTful API <http-restful-api-secure-storage>` has endpoints for
the management and creation, retrieval, and deletion of secure keys.
