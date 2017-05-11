/*
 * Copyright © 2017 Cask Data, Inc.
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
 */

import DataPrepStore from 'components/DataPrep/store';

export function directiveRequestBodyCreator(directivesArray, wsId) {
  let workspaceId = wsId || DataPrepStore.getState().dataprep.workspaceId;

  return {
    version: 1.0,
    workspace: {
      name: workspaceId,
      results: 100
    },
    recipe: {
      directives: directivesArray
    },
    sampling: {
      method: "FIRST",
      limit: 1000
    }
  };
}

export function isCustomOption(selectedOption) {
  return selectedOption.substr(0, 6) === 'CUSTOM';
}

export function setPopoverOffset(element, header_height = 50, footer_height = 54) {
  let elem = element;
  let elemBounding = elem.getBoundingClientRect();

  const FOOTER_HEIGHT = footer_height;

  let popover = document.getElementsByClassName('second-level-popover');
  let popoverHeight = popover[0].getBoundingClientRect().height;
  let tableContainerScroll = document.getElementById('dataprep-table-id').scrollTop;
  let popoverMenuItemTop = elemBounding.top;
  let bodyBottom = document.body.getBoundingClientRect().bottom + FOOTER_HEIGHT; // Since we have footer absolutely positioned.

  let diff = bodyBottom - (popoverMenuItemTop + popoverHeight) - tableContainerScroll;

  if (diff < 0) {
    diff = diff - 5; // Give that extra margin at the bottom just so that the menu doesn't stick to the bottom of the browser.
    popover[0].style.top = `${diff}px`;
  } else {
    popover[0].style.top = 0;
  }
}
