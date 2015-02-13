/*
 * version-menu.js
 * ~~~~~~~~~~~~~~~
 *
 * JavaScript for generating 
 *
 * :copyright: © Copyright 2015 Cask Data, Inc.
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
 *  versionscallback({ "development":[["2.7.0-SNAPSHOT", "2.7.0"], ["2.6.0-SNAPSHOT","2.6.0"],], "current": ["2.5.2", "2.5.2"], "versions": [["2.5.1", "2.5.1"], ["2.5.0", "2.5.0"],] });
 * 
 * list of development versions; one current version; list of additional versions
 * the tool versionscallback-gen.py will generate this from scanning a directory
 * 
 */

(function() {
  var versionsURL = 'http://docs.cask.co/cdap/';
  var versionID = 'select-version';
  var buildURL = (function(dir){
    return versionsURL + dir + '/en/';
  });
  var writelink = (function(dir, label){
    document.write('<option value="' + buildURL(dir) + '">Version ' + label + '</option>');
  });
  window.versionscallback = (function(data){
    if (data) {
      document.write('<li class="versions">');
      document.write('<select id="' + versionID + '" onmousedown="window.currentversion=this.value;" onchange="window.gotoVersion(\'' + versionID + '\')">');
    }
    var ess = "s";
    if (data.development && data.development.length > 0) {
      if (data.development.length == 1) {
        ess = "";
      }
      document.write('<optgroup label="Development Release' + ess +'">');          
      var i;
      for (i in data.development) {
        writelink(data.development[i][0], data.development[i][1]);
      }
      document.write('</optgroup>');
    }
      document.write('<optgroup label="Current Release">');
    if (data.current && data.current.length > 0) {
      writelink(data.current[0], data.current[1]);
      document.write('</optgroup>');
    }
    if (data.older && data.older.length > 0) {
      ess = "s";
      if (data.older.length == 1) {
        ess = "";
      }
      document.write('<optgroup label="Older Release' + ess + '">');
      var j;
      for (j in data.older) {
        writelink(data.older[j][0], data.older[j][1]);
      }
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
