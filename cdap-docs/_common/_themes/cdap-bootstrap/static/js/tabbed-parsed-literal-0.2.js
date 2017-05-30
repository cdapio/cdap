/*
 * tabbed-parsed-literal.js
 * ~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * JavaScript for generating a tabbed literal parsed block in rst
 *
 * :copyright: © Copyright 2016-2017 Cask Data, Inc.
 * :license: Apache License, Version 2.0
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Version 0.2
 */

var cdapDocumentationTabsets = {};
var CDAP_DOCUMENTATION_LOCAL_STORAGE = 'cdap-documentation-tabsets';
var CLASSNAME_TP = '.tabbed-parsed-literal.dependent-';

function changeExampleTab(tab, mapping, tabID, tabSetID) {
  return function(e) {
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $(CLASSNAME_TP + tabSetID + ' .tab-pane').removeClass('active');
    $(CLASSNAME_TP + tabSetID + ' .example-tab').removeClass('active');
    if (tab != mapping[tab]) {
      $(CLASSNAME_TP + tabSetID).not('#' + tabID).find('.tab-pane-'+ mapping[tab]).addClass('active');
      $(CLASSNAME_TP + tabSetID).not('#' + tabID).find('.example-tab-'+ mapping[tab]).addClass('active');
    }
    $(CLASSNAME_TP + tabSetID + ' .tab-pane-' + tab).addClass('active');
    $(CLASSNAME_TP + tabSetID + ' .example-tab-' + tab).addClass('active');
    $(document).scrollTop($(this).offset().top - scrollOffset);
    cdapDocumentationTabsets[tabSetID] = mapping[tab];
    localStorage.setItem(CDAP_DOCUMENTATION_LOCAL_STORAGE, JSON.stringify(cdapDocumentationTabsets));
  }
}

jQuery(document).ready(function() {
   $(window).load(function() {
    var example;
    try {
      var tabsets = $.parseJSON(localStorage.getItem(CDAP_DOCUMENTATION_LOCAL_STORAGE));
    } catch(e) {
      console.log('Unable to set using local storage: bad JSON');
      return;
    }
    if (tabsets) {
      cdapDocumentationTabsets = tabsets;
      for (var tabSetID in tabsets) {
        if (tabsets.hasOwnProperty(tabSetID)) {
          var tab = tabsets[tabSetID];
          var tabs = $(CLASSNAME_TP + tabSetID + ' .example-tab-' + tab);
          if (tab != '' && (tabs.length > 0)) {
            try {
              tabs[0].click(changeExampleTab(tab));
            } catch (e) {
              console.log('Unable to set using local storage: ' + tab);
            }
//           } else {
//             console.log('Unable to set using local storage (no tabs): ' + tab);
          }
        }
      }
    } else {
      console.log('Unable to set using local storage (no tabsets)');
    }
  });
});
