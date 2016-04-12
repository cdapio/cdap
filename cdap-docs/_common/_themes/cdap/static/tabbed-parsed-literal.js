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

function changeExampleTab(example, mapping, tabID) {
  return function(e) {
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $(".dependent .tab-pane").removeClass("active");
    $(".dependent .example-tab").removeClass("active");
    if (example != mapping[example]) {
      $('.dependent').not('#' + tabID).find('.tab-pane-'+ mapping[example]).addClass("active");
      $('.dependent').not('#' + tabID).find('.example-tab-'+ mapping[example]).addClass("active");
    }
    $(".dependent .tab-pane-" + example).addClass("active");
    $(".dependent .example-tab-" + example).addClass("active");
    $(document).scrollTop($(this).offset().top - scrollOffset);
    localStorage.setItem("cdap-documentation-tab", mapping[example]);
  }
}

jQuery(document).ready(function() {
  var example = localStorage.getItem("cdap-documentation-tab");
  var tabs = $(".dependent .example-tab-" + example);
  if (example && tabs) {
    try {
      $(".dependent .example-tab-" + example)[0].click(changeExampleTab(example));
    } catch (e) {
      console.log("Unable to set using local storage: " + example);
    }
  } else {
    console.log("Unable to set using local storage: " + example);
  }
});
