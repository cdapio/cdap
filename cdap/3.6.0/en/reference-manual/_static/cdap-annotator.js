/*
 * cdap-annotator.js
 *
 * Adding Annotator JavaScript utilities for all documentation.
 *
 * FIXME: Hard-coded server
 * FIXME: To get this to work, I altered cdap-annotator-full.js (a replacement for annotator-full.js)
 *       AnnotateItPermissions.prototype.options.userAuthorize to always returns true (line 2562).
 *       However, this seems to have introduced a bug of multiple checkboxes in the popup window
 *       and the name 'alice' is always used for the user...
 * I also altered lines 1659 & 1660 to fix a window-sizing issue
 *
 */

$(document).ready(function() {
  var content = $('#documentwrapper').annotator();
  var pageURI = $(location).attr('href');
  content.annotator('setupPlugins', {}, {
                   // Disable selected plugins
                   Auth: false,
                   Store: false,
                   Permissions: false,
                 });
  content.annotator('addPlugin', 'Store', {
    // The endpoint of the store on your server.
    prefix: 'http://annotationstorage10595-1000.dev.continuuity.net:5000',
    
    // Attach the uri of the current page to all annotations to allow search.
    annotationData: {
      'uri': pageURI
    },
    
    // This will perform a "search" action when the plugin loads. Will
    // request the last 20 annotations for the current url.
    // eg. /store/endpoint/search?limit=20&uri=http://this/document/only
    loadFromSearch: {
      'limit': 20,
      'uri': pageURI
    }   
  });
  content.annotator('addPlugin', 'Permissions', {
    user: 'doc-eng',
    permissions: {
      'read':   [],
      'update': [],
      'delete': [],
      'admin':  []
    }
  });
});
