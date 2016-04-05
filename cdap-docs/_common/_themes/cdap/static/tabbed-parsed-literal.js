/*
 * tabbed-parsed-literal.js
 * ~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * JavaScript for generating a tabbed literal parsed block in rst
 *
 * :copyright: © Copyright 2016 Cask Data, Inc.
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
 * 
 */

var cdapDocumentationTabsets = {};

function changeExampleTab(example, mapping, tabID, tabSetID) {
  return function(e) {
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $(".dependent-" + tabSetID + " .tab-pane").removeClass("active");
    $(".dependent-" + tabSetID + " .example-tab").removeClass("active");
    if (example != mapping[example]) {
      $('.dependent-' + tabSetID).not('#' + tabID).find('.tab-pane-'+ mapping[example]).addClass("active");
      $('.dependent-' + tabSetID).not('#' + tabID).find('.example-tab-'+ mapping[example]).addClass("active");
    }
    $(".dependent-" + tabSetID + " .tab-pane-" + example).addClass("active");
    $(".dependent-" + tabSetID + " .example-tab-" + example).addClass("active");
    $(document).scrollTop($(this).offset().top - scrollOffset);
    cdapDocumentationTabsets[tabSetID] = mapping[example];
    localStorage.setItem("cdap-documentation-tabsets", JSON.stringify(cdapDocumentationTabsets));
  }
}

jQuery(document).ready(function() {
   $(window).load(function() {
    var example;
    try {
      var tabsets = $.parseJSON(localStorage.getItem("cdap-documentation-tabsets"));
    } catch(e) {
      console.log("Unable to set using local storage: bad JSON");
      return;
    }
    if (tabsets) {
      cdapDocumentationTabsets = tabsets;
      for (var tabSetID in tabsets) {
        if (tabsets.hasOwnProperty(tabSetID)) {
          var tab = tabsets[tabSetID];
          var tabs = $(".dependent-" + tabSetID + " .example-tab-" + tab);
          if (tab && tabs) {
            try {
              tabs[0].click(changeExampleTab(tab));
            } catch (e) {
              console.log("Unable to set using local storage: " + tab);
            }
          } else {
            console.log("Unable to set using local storage (no tabs): " + tab);
          }
        }
      }
    } else {
      console.log("Unable to set using local storage (no tabsets)");  
    }
  });
});
