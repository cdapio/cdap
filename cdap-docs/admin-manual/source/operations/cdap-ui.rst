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
---------------------------------
Certain properties in the UI can be customized. The ``ui/server/config/themes/default.json`` file contains these properties, along with their default values. As of version 1.0, here is the list of properties that can be customized, and their expected values:

.. highlight:: json-ellipsis

.. list-table::
   :widths: 15 30 20 30
   :header-rows: 1

   * - Property Name
     - Property Attributes
     - Description
     - Example

   * - ``brand-primary-color``
     - No attributes
     - The brand color to be used throughout UI. Can be in hex/rgb/rgba format.
     - .. container:: copyable copyable-text

         ::

          "brand-primary-color": "#3b78e7"

   * - ``navbar-color``
     - No attributes
     - The background color of the top nav bar. Can be in hex/rgb/rgba format.
     - .. container:: copyable copyable-text

         ::

          "navbar-color": "rgba(51, 51, 51, 1)"

   * - ``font-family``
     - No attributes
     - Comma-separated list of font families to be used in UI, in decreasing order of priority.
     - .. container:: copyable copyable-text

         ::

          "font-family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"

   * - ``product-name``
     - No attributes
     - The product name to be used in UI. This will displayed on page titles, 'Welcome' modal, and the 'About' modal.
     - .. container:: copyable copyable-text

         ::

          "product-name": "CDAP"

   * - ``product-description``
     - No attributes
     - The product description to be used in 'Welcome' modal.
     - .. container:: copyable copyable-text

         ::

          "product-description": "CDAP is an open source framework that simplifies data application development, data integration, and data management."

   * - ``product-logo-navbar``
     - - ``type``:
         - ``link``: user is expected to provide a link to the icon image in the ``url`` attribute inside the ``arguments`` map. Path should be from ``ui/cdap_dist`` directory.
         - ``inline``: user is expected to provide a base64 encoded image in the ``data`` attribute inside the ``arguments`` map.
       - ``arguments``:
         - ``url``
         - ``data``
     - The logo to be displayed on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "product-logo-navbar": {
              "type": "link",
              "arguments": {
                 "url": "/cdap_assets/img/company_logo.png"
              }
          }
   * - ``product-logo-about``
     - - ``type``:
         - ``link``: user is expected to provide a link to the icon image in the ``url`` attribute inside the ``arguments`` map. Path should be from ``ui/cdap_dist`` directory.
         - ``inline``: user is expected to provide a base64 encoded image in the ``data`` attribute inside the ``arguments`` map.
       - ``arguments``:
         - ``url``
         - ``data``
     - The logo to be displayed in the 'About' modal.
     - .. container:: copyable copyable-text

         ::

          "product-logo-about": {
              "type": "link",
              "arguments": {
                 "url": "/cdap_assets/img/CDAP_darkgray.png"
              }
          },

   * - ``favicon-path``
     - No attributes
     - Path to the image to be used as favicon.
     - .. container:: copyable copyable-text

         ::

          "favicon-path": "/cdap_assets/img/favicon.png"


   * - ``footer-text``
     - No attributes
     - Text content to displayed on the footer component.
     - .. container:: copyable copyable-text

         ::

          "footer-text": "Licensed under the Apache License, Version 2.0"

   * - ``footer-link``
     - No attributes
     - Link to route to when user clicks on footer text.
     - .. container:: copyable copyable-text

         ::

          "footer-link": "https://www.apache.org/licenses/LICENSE-2.0"

   * - ``dashboard``
     - No attributes
     - Whether to show 'Dashboard' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "dashboard": true

   * - ``reports``
     - No attributes
     - Whether to show 'Reports' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "reports": true

   * - ``data-prep``
     - No attributes
     - Whether to show 'Preparation' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "data-prep": true

   * - ``pipelines``
     - No attributes
     - Whether to show 'Pipelines' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "pipelines": true

   * - ``analytics``
     - No attributes
     - Whether to show 'Analytics' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "analytics": true

   * - ``rules-engine``
     - No attributes
     - Whether to show 'Rules Engine' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "rules-engine": true

   * - ``metadata``
     - No attributes
     - Whether to show 'Metadata' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "metadata": true

   * - ``hub``
     - No attributes
     - Whether to show 'Hub' feature on the nav bar.
     - .. container:: copyable copyable-text

         ::

          "hub": true

   * - ``ingest-data``
     - No attributes
     - Whether to show 'Ingest Data' feature in the Preparation app.
     - .. container:: copyable copyable-text

         ::

          "ingest-data": true


   * - ``add-namespace``
     - No attributes
     - Whether the user can add a new namesace in the UI.
     - .. container:: copyable copyable-text

         ::

          "add-namespace": true

However, it is not recommended to overwrite values in ``default.json`` for customizations, since these values will be reverted when CDAP is upgraded. Instead of doing this, the user is encouraged to create their own theme file using this spec, and link to this file. To do so, add this property to ``cdap-site.xml``::

    {
      <property>
        <name>ui.theme.file</name>
        <value>[path-to-theme-file]</value>
        <description>
          File containing the theme to be used in UI
        </description>
      </property>
    }

After updating this property (or changing values in ``default.json``), the changes will be reflected in CDAP UI after CDAP is restarted.
