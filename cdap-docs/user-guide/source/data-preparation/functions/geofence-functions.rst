.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==================
Geofence Functions
==================

These are functions for detecting if the location point is inside the
given set of geofences. The function can be used in the directive
``filter-row-if-false``, ``filter-row-if-true``, ``filter-row-on``,
``set column`` or ``send-to-error``.

Pre-requisite
-------------

The Geofences should be represented in geoJson format. The location
coordinates should be represented as Double type values .

Example Data
------------

Upload to the workspace ``body`` an input record such as:

::

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {},
          "geometry": {
            "type": "Polygon",
            "coordinates": [
              [
                [
                  -122.05870628356934,
                  37.37943348292772
                ],
                [
                  -122.05724716186525,
                  37.374727268782294
                ],
                [
                  -122.04634666442871,
                  37.37493189292912
                ],
                [
                  -122.04608917236328,
                  37.38175237839049
                ],
                [
                  -122.05870628356934,
                  37.37943348292772
                ]
              ]
            ]
          }
        }
      ]
    }

Once such a record is loaded, either the direct values of location
coordinates could be used or could be set as columns

::

      set column latitude 37.37220352507352
      set column longitude -122.04608917236328

Geofence Function
-----------------

The function returns ``true`` if the location point is inside any of the
given set of geofences, ``false`` otherwise.

+------------------------+----------------------+-------------------------------+
| Function               | Condition            | Example                       |
+========================+======================+===============================+
| ``locationInFence(doub | Tests if point is    | ``send-to-error !geo:inFence( |
| le, double, String)``  | inside the fences    | latitude,longitude,body)``    |
+------------------------+----------------------+-------------------------------+
