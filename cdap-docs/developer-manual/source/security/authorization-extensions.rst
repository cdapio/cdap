.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _authorization-extensions:

========================
Authorization Extensions
========================

Authorization backends in CDAP are implemented as extensions, and are loaded and executed in their
own Java classloader. 

The reason for implementing them as extensions is that a variety of existing authorization
systems can then be plugged into CDAP, and can execute using their own classloader without
having to worry about conflicts with CDAP's system classloader. Each authorization
extension is designed to be a fully self-contained JAR file, packaged with the required
versions of all its dependencies.

This is similar to how CDAP applications are packaged. In fact, users can use the CDAP
application archetype to write their own authorization extension for CDAP. Having the
required dependencies loaded from a separate class loader constructed using the provided
JAR file ensures that there will be no conflicts, even if CDAP itself uses a different
version of the library than the one that the extension requires.

Writing Your Own Authorization Extension
----------------------------------------
CDAP provides an authorization SPI (Service Provider Interface) for users to implement their own authorization
extensions. To implement an authorization extension:

- Create a Maven project using the CDAP Application Archetype, as described in :ref:`Creating an Application <dev-env>`.

- Implement your authorization extension by extending the ``AbstractAuthorizer`` class.

- This class must be specified as the ``Main-Class`` attribute in the extension JAR's
  manifest file. This will be done automatically if you specify the ``app.main.class``
  property to be the fully qualified class name in the ``pom.xml`` of the Maven project
  generated using the archetype.
  
- The class that extends ``AbstractAuthorizer`` must have a default constructor, as that
  default constructor is the one that will be invoked by CDAP.

- All dependencies of the class must be packaged within the JAR file containing the
  authorizer class. This is also done automatically by the CDAP Application Archetype.

- The ``AbstractAuthorizer`` class provides lifecycle methods for authorization
  extensions. Any initialization code should be implemented by overriding the ``initialize``
  method. Cleanup code should be implemented by overriding the ``destroy`` method.

- The ``initialize`` method provides an ``AuthorizationContext`` object. This object can
  be used to obtain access to |---| and perform admin operations on |---| CDAP Datasets and Secure
  Keys.

- If the authorization extension requires configuration parameters, they are provided in
  the ``AuthorizationContext`` object as a Java ``Properties`` object via the
  ``getProperties`` method. The ``Properties`` object is populated with configuration
  parameters from ``cdap-site.xml`` with the prefix ``security.authorization.extension.config``.
  
  **Note:** In the ``Properties`` object, these configuration parameters are available
  with the ``security.authorization.extension.config`` prefix removed. For example:
  ``security.authorization.extension.config.example.property`` in the ``cdap-site.xml``
  will be available as ``example.property`` in the ``Properties`` object.

For an example of implementing an authorization extension, please refer to the
:cdap-security-extn-source-github:`Apache Sentry Authorization Extension
<cdap-sentry/cdap-sentry-extension/cdap-sentry-binding/src/main/java/co/cask/cdap/security/authorization/sentry/binding/SentryAuthorizer.java>`
as well as its documentation at :ref:`Integrations: Apache Sentry <apache-sentry>`.
