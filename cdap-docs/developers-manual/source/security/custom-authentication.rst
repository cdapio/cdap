.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _custom-authentication:

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

To make your custom handler class available to the authentication service, copy your
packaged jar file (and any additional dependency jars) to the ``security/lib/`` directory
within your CDAP installation (typically under ``/opt/cdap``).
