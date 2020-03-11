/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import PropTypes from 'prop-types';
import * as Selection from '@simonwep/selection-js';
require('./SelectionBox.scss');

interface ISelectionMoveObj {
  selected: string[];
}

interface ISelectionBoxProps {
  boundaries: string[];
  selectables: string[];
  selectionClass: string;
  onSelectionStart?: () => void;
  onSelectionMove?: (selectionObj: ISelectionMoveObj) => void;
  onSelectionEnd?: () => void;
  toggleSelection?: boolean;
}

const SELECTION_CSS_CLASS = 'selection-box-selection-container';
const SELECTABLES = ['.box-wrapper > div'];
const BOUNDARIES = ['.box-wrapper'];
export default function SelectionBox(props: ISelectionBoxProps) {
  const {
    boundaries,
    selectables,
    selectionClass,
    onSelectionStart,
    onSelectionMove,
    onSelectionEnd,
    toggleSelection,
  } = props;
  const [selection, setSelection] = React.useState(null);

  React.useEffect(() => {
    setSelection(
      Selection.create({
        // Class for the selection-area
        class: selectionClass || SELECTION_CSS_CLASS,

        // All elements in this container can be selected
        selectables: selectables || SELECTABLES,

        // The container is also the boundary in this case
        boundaries: boundaries || BOUNDARIES,
      })
    );
  }, []);

  React.useEffect(() => {
    if (!selection) {
      return;
    }
    if (toggleSelection) {
      selection.enable();
    } else {
      selection.disable();
    }
  }, [toggleSelection]);

  React.useEffect(() => {
    if (!selection) {
      return;
    }

    selection.on('start', () => {
      if (onSelectionStart) {
        onSelectionStart();
      }
    });
    selection.on('move', ({ selected, changed: { added, removed } }) => {
      const selectedNodes = [];
      for (const el of selected) {
        selectedNodes.push(el.id);
      }
      if (onSelectionMove && (added.length > 0 || removed.length > 0)) {
        onSelectionMove({ selected: selectedNodes });
      }
    });
    selection.on('stop', () => {
      if (onSelectionEnd) {
        onSelectionEnd();
      }
    });

    return () => {
      if (selection) {
        selection.destroy();
      }
    };
  }, [selection]);
  return null;
}

(SelectionBox as any).propTypes = {
  boundaries: PropTypes.string.isRequired,
  selectables: PropTypes.string.isRequired,
  selectionClass: PropTypes.string,
  onSelectionStart: PropTypes.func,
  onSelectionMove: PropTypes.func,
  onSelectionEnd: PropTypes.func,
  toggleSelection: PropTypes.bool,
};
