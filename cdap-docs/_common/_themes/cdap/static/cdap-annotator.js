/*
 * cdap-annotator.js
 *
 * Adding Annotator JavaScript utilities for all documentation.
 *
 * FIXME: hard-coded server address
 */

$(document).ready(function() {
  var content = $('#documentwrapper').annotator();
  content.annotator('setupPlugins', {}, {
    Store: false
  });
  content.annotator('addPlugin', 'Store', {
    // The endpoint of the store on your server.
    prefix: 'http://annotationstorage10595-1000.dev.continuuity.net:5000',
  });

});

