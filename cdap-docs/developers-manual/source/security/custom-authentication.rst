.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _developers-custom-authentication:

=====================
Custom Authentication
=====================

.. highlight:: java

To provide a custom authentication mechanism, create your own authentication handler by
extending ``AbstractAuthenticationHandler`` and implementing its abstract methods::

  public class CustomAuthenticationHandler extends AbstractAuthenticationHandler {

    @Inject
    public CustomAuthenticationHandler(CConfiguration configuration) {
      super(configuration);
    }

    @Override
    protected LoginService getHandlerLoginService() {
      // ...
    }

    @Override
    protected IdentityService getHandlerIdentityService() {
      // ...
    }

    @Override
    protected Configuration getLoginModuleConfiguration() {
      // ...
    }
  }


An example of an ``AuthenticationHandler`` can be found in the CDAP source code
for `LDAPAuthenticationHandler.java <https://github.com/caskdata/cdap/blob/develop/cdap-security/src/main/java/co/cask/cdap/security/server/LDAPAuthenticationHandler.java>`__.

To configure the custom authentication handler, see the Administration Manual’s
:ref:`installation-custom-authentication` section.
