/*
 * copy-to-clipboard.js
 * ~~~~~~~~~~~~~~~~~~~~
 *
 * JavaScript for generating 
 *
 * :copyright: (C) Copyright 2016-2017 Cask Data, Inc.
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
 * Example usage:
 *
 * <span class="copyable">$ <span class="copyable-text">cdap cli start service HelloWorld.Greeting</span></span>
 *
 * Example in rst:
 *
 * .. container:: copyable copyable-text
 *
 *    Text for paragraph to be copyable.
 *
 * version 1.0.2
 *
 * Requires JQuery
 * 
 */

jQuery(document).ready(function() {
  var tooltip, hidetooltiptimer;
  var optionkeyed = false;
  var copyables = document.getElementsByClassName('copyable');
  for (var i = 0; i < copyables.length; i++) {
  
    copyables[i].addEventListener('mouseup', function(e){
      var e = e || event // equalize event object between modern and older IE browsers
      var target = e.target || e.srcElement // get target element mouse is over
      if (target.className != 'copyable-text' && target.parentNode.className.includes('copyable-text')) {
        target = target.parentNode;
      } else if ($(this)[0].className.includes('copyable-text')) {
        target = $(this)[0]
      }
      if (target.className.includes('copyable-text') && !optionkeyed) {
        var copysuccess = copySelectionText()
        if (copysuccess) {
          showTooltip(e, 'Copied!');
          selectElementText(target)
        }
      }
    }, false);

    copyables[i].addEventListener('mouseover', function(e){
      var e = e || event // equalize event object between modern and older IE browsers
      var target = e.target || e.srcElement // get target element mouse is over
      if (target.className != 'copyable-text' && target.parentNode.className.includes('copyable-text')) {
        target = target.parentNode;
      } else if ($(this)[0].className.includes('copyable-text')) {
        target = $(this)[0]
      }
      if (target.className.includes('copyable-text') && !optionkeyed){
        selectElementText(target) // select the element's text we wish to read
      
        var actionMsg, disableKey;
        if (/Mac/i.test(navigator.userAgent)) {
          actionMsg = 'Press ⌘-';
          disableKey = 'Option';
        } else {
          actionMsg = 'Press Ctrl-';
          disableKey = 'Alt';
        }
        if (navigator.userAgent.indexOf('Safari') != -1 && 
              navigator.userAgent.indexOf('Chrome') == -1) {
          showTooltip(e, actionMsg + 'C to copy (use the ' + disableKey + ' key to disable)');
        } else {
          showTooltip(e, 'Click to copy (use the ' + disableKey + ' key to disable)');
        }
      }
    }, false);
    
    document.addEventListener('keydown', function(e){
      var e = e || event // equalize event object between modern and older IE browsers
      optionkeyed = (e.which == 18);
    }, false);
    
    document.addEventListener('keyup', function(e){
      var e = e || event // equalize event object between modern and older IE browsers
      optionkeyed = optionkeyed && !(e.which == 18);
    }, false);
    
  }

  function createCopyableTooltip(){ // call this function ONCE at the end of page to create tool tip object
    tooltip = document.createElement('div')
    tooltip.style.cssText = 
      'position:absolute; background:#E0E0E0; color:black; padding:4px; z-index:10000;'
      + 'border-radius:2px; box-shadow:3px 3px 3px rgba(0,0,0,.4); opacity:0;'
      + 'transition:opacity 0.3s; font-family:"Helvetica"; font-size:12px; '
    tooltip.innerHTML = ''
    document.body.appendChild(tooltip)
  }

  function showTooltip(e, label){
    var evt = e || event
    clearTimeout(hidetooltiptimer)
    tooltip.innerHTML = label
    tooltip.style.left = evt.pageX - 10 + 'px'
    tooltip.style.top = evt.pageY + 15 + 'px'
    tooltip.style.opacity = 1
    hidetooltiptimer = setTimeout(function(){
      tooltip.style.opacity = 0
    }, 1000)
  }

  function selectElementText(el){
    var range = document.createRange() // create new range object
    range.selectNodeContents(el) // set range to encompass desired element text
    var selection = window.getSelection() // get Selection object from currently user selected text
    selection.removeAllRanges() // unselect any user selected text (if any)
    selection.addRange(range) // add range to Selection object to select it
  }

  function copySelectionText(){
    var copysuccess // var to check whether execCommand successfully executed
    try {
      copysuccess = document.execCommand("copy") // run command to copy selected text to clipboard
    } catch(e){
      copysuccess = false
    }
    return copysuccess
  }

  createCopyableTooltip();
});
