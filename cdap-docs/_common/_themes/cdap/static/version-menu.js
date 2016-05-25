/*
 * version-menu.js
 * ~~~~~~~~~~~~~~~
 *
 * JavaScript for generating 
 *
 * :copyright: © Copyright 2015-2016 Cask Data, Inc.
 * :license: Apache License, Version 2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Requires a JSONP file at http://docs.cask.co/cdap/json-versions.js in the format:
 * 
 * versionscallback({'development': [['3.5.0-SNAPSHOT', '3.5.0']], 'current': ['3.4.1', '3.4.1', '2016-05-12'], 'timeline': [['0', '3.4.0', '2016-04-29', ' (100 days)'], ['1', '3.4.1', '2016-05-12', ' (13 days)'], ['0', '3.3.0', '2016-01-20', ' (119 days)'], ['1', '3.3.1', '2016-02-19', ' (30 days)'], ['1', '3.3.2', '2016-03-07', ' (17 days)'], ['1', '3.3.3', '2016-04-15', ' (39 days)'], ['1', '3.3.4', '2016-05-19', ' (34 days)'], ['0', '3.2.0', '2015-09-23', ' (51 days)'], ['1', '3.2.1', '2015-10-21', ' (28 days)'], ... ['2.6.0', '2.6.0', '2015-01-10', '']]});
 * 
 * list of development versions; one current version; list of additional versions
 *
 * version 0.3
 * 
 */

(function() {
  var versionsURL = 'http://docs.cask.co/cdap/';
  var versionID = 'select-version';
  var buildURL = (function(dir){
    return versionsURL + dir + '/en/';
  });
  var writeLink = (function(dir, label){
    document.write('<option value="' + buildURL(dir) + '">' + label + '</option>');
  });
  var writeVersionLink = (function(dir, label){
    writeLink(dir, 'Version ' + label);
  });
  window.versionscallback = (function(data){
    if (data) {
      document.write('<li class="versions">');
      document.write('<select id="' + versionID + '" onmousedown="window.currentversion=this.value;" onchange="window.gotoVersion(\'' + versionID + '\')">');
    }
    var ess;
    if (data.development && data.development.length > 0 && data.development[0]) {
      ess = (data.development.length == 1) ? "" : "s" ;
      document.write('<optgroup label="Development Release' + ess +'">');          
      if (data.development && data.development.length > 0) {
        var i;
        for (i in data.development) {
          writeLink(data.development[i][0], 'Develop (' + data.development[i][1] + ')');
          writeVersionLink(data.development[i][0], data.development[i][1]);
        }
      }
      document.write('</optgroup>');
    } else {
      writeLink('develop', 'Develop');
    }
    document.write('<optgroup label="Current Release">');
    if (data.current && data.current.length > 1 && data.current[0]) {
      writeLink('current', 'Current (' + data.current[1] + ')')
      writeVersionLink(data.current[0], data.current[1]);
    } else {
      writeLink('current', 'Current');
    }
    document.write('</optgroup>');
    if (data.older && data.older.length > 0 && data.older[0]) {
      ess = (data.older.length == 1) ? "" : "s" ;
      document.write('<optgroup label="Older Release' + ess + '">');
      var j;
      for (j in data.older) {
        if (parseInt(data.older[j][3]) === 1) {
          writeVersionLink(data.older[j][0], data.older[j][1]);
        }
      }
      document.write('<option value="' + versionsURL + '">All Releases</option>');
      document.write('</optgroup>');
    }
    if (data) {
      document.write('</select>');
    }
  });
  window.gotoVersion = (function(id) {
    var node = document.getElementById( id );
    // Check to see if valid node and if node is a SELECT form control
    if( node && node.tagName == "SELECT" ) {
      // Get the web page defined by the VALUE attribute of the OPTION element
      var location = node.options[node.selectedIndex].value;
      // Reset select menu to current page
      node.value = window.currentversion;
      // Go to web page
      window.location.href = location;
    }
  });
  window.setVersion = (function(version) {
    var node = document.getElementById(versionID);
    // Check to see if valid node and if node is a SELECT form control
    if( node && node.tagName == "SELECT" ) {
      node.value = buildURL(version);
    }
  });
})();
