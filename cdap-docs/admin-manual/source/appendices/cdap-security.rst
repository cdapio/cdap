.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _appendix-cdap-security.xml:

============================================
Appendix: ``cdap-security.xml``
============================================

Here are the parameters that can be defined in the ``cdap-security.xml`` file,
their default values, descriptions, and notes.

For information on configuring the ``cdap-security.xml`` file and CDAP for security,
see the :ref:`configuration-security` section.

..   :widths: 20 20 30

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter name
     - Default Value
     - Description
   * - ``dashboard.ssl.cert``
     -
     - SSL certificate file to be used for the CDAP UI
   * - ``dashboard.ssl.key``
     -
     - SSL key file corresponding to the SSL certificate specified in ``dashboard.ssl.cert``
   * - ``router.ssl.keystore.keypassword``
     -
     - Key password to the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``router.ssl.keystore.password``
     -
     - Password to the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``router.ssl.keystore.path``
     -
     - Path to the Java keystore file containing the certificate used for HTTPS on the CDAP Router
   * - ``router.ssl.keystore.type``
     - ``JKS``
     - Type of the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.keypassword``
     -
     - Key password to the Java keystore file specified in ``security.auth.server.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.password``
     -
     - Password to the Java keystore file specified in ``security.auth.server.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.path``
     -
     - Path to the Java keystore file containing the certificate used for HTTPS on the CDAP
       Authentication Server
   * - ``security.auth.server.ssl.keystore.type``
     - ``JKS``
     - Type of the Java keystore file specified in ``security.auth.server.ssl.keystore.path``
