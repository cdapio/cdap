.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2018 Cask Data, Inc.

.. _cdap-console:
.. _cdap-ui:

=======
CDAP UI
=======

The CDAP UI is a web UI for deploying and managing applications and datasets. In addition, it also contains UIs
for the data pipeline and data preparation applications.

Supported Browsers and Components
---------------------------------
+-------------------+--------------------------------+---------------------+
| Browser           | Platform                       | Supported Versions  |
+===================+================================+=====================+
| Chrome            | Apple OS X, Microsoft Windows  | 55.0 and higher     |
+-------------------+--------------------------------+---------------------+
| Firefox           | Apple OS X, Microsoft Windows  | 50.0 and higher     |
+-------------------+--------------------------------+---------------------+
| Safari            | Apple OS X, Microsoft Windows  | 10 and higher       |
+-------------------+--------------------------------+---------------------+
|                   |                                |                     |
+-------------------+--------------------------------+---------------------+


+-------------------+--------------------------------+---------------------+
| Component         | Platform                       | Supported Versions  |
+===================+================================+=====================+
| Node.js           | Various                        | 8.7 and higher      |
+-------------------+--------------------------------+---------------------+

**Note:** Supported component versions shown in these tables are those that we have tested
and are confident of their suitability and compatibility. Earlier versions (and different
suppliers) of components may work, but have not necessarily been either tested or
confirmed compatible.

UI Customization
----------------
Certain features in the UI can be customized. The ``<CDAP_HOME>/ui/server/config/themes/default.json`` file contains these
features, along with their default values. As of version 1.0, here is the list of features that can be customized,
and their expected values:

.. list-table::
   :widths: 15 20 30
   :header-rows: 1

   * - Property Name
     - Description
     - Example

   * - ``brand-primary-color``
     - The primary brand color to be used across UI. This is most often used as the color of the 'active' text selection
       e.g. the active connection in Preparation app. This will also determine the color of some icons, e.g. the loading
       icon used across UI. Can be in hex/rgb/rgba format.
     - .. container:: highlight

        .. parsed-literal::

          "brand-primary-color": "#3b78e7"


   * - ``navbar-color``
     - The background color of the top navigation bar. Can be in hex/rgb/rgba format.
     - .. container:: highlight

        .. parsed-literal::

          "navbar-color": "rgba(51, 51, 51, 1)"

   * - ``font-family``
     - Comma-separated list of font families to be used in UI, in decreasing order of priority.
     - .. container:: highlight

        .. parsed-literal::

          "font-family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"

   * - ``product-name``
     - The product name to be used in UI. This will displayed on page titles, 'Welcome' modal, and the 'About' modal.
     - .. container:: highlight

        .. parsed-literal::

          "product-name": "CDAP"

   * - ``product-description``
     - The product description to be used in 'Welcome' modal.
     - .. container:: highlight

        .. parsed-literal::

          "product-description": "CDAP is an open source framework that simplifies data application development, data
          integration, and data management."

   * - ``product-logo-navbar``
     - The logo to be displayed on the navigation bar. This can be specified using the ``type`` and ``arguments`` fields.
       If value of ``type`` field is ``link``, then the user is expected to specify the path to the logo image in a
       ``url`` attribute inside the ``arguments`` map, and this should be absolute path from
       ``<CDAP_HOME>/ui/cdap_dist``. If value of ``type`` field is ``inline``, the user is expected to provide a base64
       encoded image in a ``data`` attribute inside the ``arguments`` map. The image can be in any standard image format
       e.g. PNG, JPEG, SVG etc.
     - .. container:: highlight

        .. parsed-literal::

          "product-logo-navbar": {
            "type": "link",
            "arguments": {
              "url": "/cdap_assets/img/company_logo.png"
            }
          }


   * - ``product-logo-about``
     - The logo to be displayed in the 'About' modal. This can be specified using the ``type`` and ``arguments`` fields.
       If value of ``type`` field is ``link``, then the user is expected to specify the path to the logo image in a
       ``url`` attribute inside the ``arguments`` map, and this should be absolute path from
       ``<CDAP_HOME>/ui/cdap_dist``. If value of ``type`` field is ``inline``, the user is expected to provide a base64
       encoded image in a ``data`` attribute inside the ``arguments`` map. The image can be in any standard image format
       e.g. PNG, JPEG, SVG etc.
     - .. container:: highlight

        .. parsed-literal::

          "product-logo-about": {
            "type": "link",
            "arguments": {
              "url": "/cdap_assets/img/CDAP_darkgray.png"
            }
          }

   * - ``favicon-path``
     - Path to the image to be used as favicon. Should be absolute path from ``<CDAP_HOME>/ui/cdap_dist``. The image can
       be in any standard image format e.g. PNG, JPEG, SVG etc.
     - .. container:: highlight

        .. parsed-literal::

          "favicon-path": "/cdap_assets/img/favicon.png"


   * - ``footer-text``
     - Text content to displayed on the footer component.
     - .. container:: highlight

        .. parsed-literal::

          "footer-text": "Licensed under the Apache License, Version 2.0"

   * - ``footer-link``
     - Link to route to when user clicks on footer text.
     - .. container:: highlight

        .. parsed-literal::

          "footer-link": "https://www.apache.org/licenses/LICENSE-2.0"

   * - ``dashboard``
     - Whether to show 'Dashboard' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "dashboard": true

   * - ``reports``
     - Whether to show 'Reports' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "reports": true

   * - ``data-prep``
     - Whether to show 'Preparation' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "data-prep": true

   * - ``pipelines``
     - Whether to show 'Pipelines' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "pipelines": true

   * - ``analytics``
     - Whether to show 'Analytics' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "analytics": true

   * - ``rules-engine``
     - Whether to show 'Rules Engine' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "rules-engine": true

   * - ``metadata``
     - Whether to show 'Metadata' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "metadata": true

   * - ``hub``
     - Whether to show 'Hub' feature on the navigation bar.
     - .. container:: highlight

        .. parsed-literal::

          "hub": true

   * - ``ingest-data``
     - Whether to show 'Ingest Data' feature in the Preparation app.
     - .. container:: highlight

        .. parsed-literal::

          "ingest-data": true


   * - ``add-namespace``
     - Whether the user can add a new namesace in the UI.
     - .. container:: highlight

        .. parsed-literal::

          "add-namespace": true

.. highlight:: xml

It is not recommended to overwrite values in ``default.json`` for customizations, since these values will be reverted
when CDAP is upgraded. Instead of this, users should create their own theme file using this spec, and link to this file.
To do so, add this property to ``cdap-site.xml``::

    <property>
      <name>ui.theme.file</name>
      <value>[path-to-theme-file]</value>
      <description>
        File containing the theme to be used in UI
      </description>
    </property>


After updating this property (or changing values in ``default.json``), the changes will be reflected in CDAP UI after
CDAP is restarted.
