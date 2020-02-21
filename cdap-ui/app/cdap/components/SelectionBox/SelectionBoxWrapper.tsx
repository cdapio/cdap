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

import * as Selection from '@simonwep/selection-js';
import React from 'react';
import withStyles from '@material-ui/core/styles/withStyles';
require('./SelectionBox.scss');

const Div = ({ classes, className = '', children = null, ...props }) => {
  return (
    <div className={`${className} ${classes.root}`} {...props}>
      {children}
    </div>
  );
};
const Container = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: 'repeat(17, 1fr)',
      gridAutoRows: 'max-content',
      gridGap: '0.4em',
      alignItems: 'flex-start',
      justifyContent: 'flex-start',
      maxHeight: '25em',
      overflow: 'auto',
      padding: '0.5em',
      height: '50%',
      width: '80%',
      margin: '0 auto',
    },
  };
})(Div);
const Box = withStyles(() => {
  return {
    root: {
      height: '3em',
      width: 'auto',
      margin: '0.2em',
      background: 'rgba(66, 68, 90, 0.075)',
      borderRadius: '0.15em',
      transition: 'all 0.3s',
      cursor: 'pointer',
      '&.selected': {
        background: 'deepskyblue',
      },
    },
  };
})(Div);

export default function SelectionBoxWrapper() {
  // const [selection, setSelection]
  React.useEffect(() => {
    const selection = Selection.create({
      // Class for the selection-area
      class: 'selection-box-selection-container',

      // All elements in this container can be selected
      selectables: ['.box-wrapper > div'],

      // The container is also the boundary in this case
      boundaries: ['.box-wrapper'],
    })
      .on('start', ({ inst }) => {
        const container = document.querySelector(`.box-wrapper`);
        if (container) {
          Array.prototype.slice
            .apply(container.children)
            .forEach((child) => child.classList.remove('selected'));
        }
        inst.clearSelection();
      })
      .on('move', ({ changed: { removed, added } }) => {
        // Add a custom class to the elements that where selected.
        for (const el of added) {
          el.classList.add('selected');
        }
        // Remove the class from elements that where removed
        // since the last selection
        for (const el of removed) {
          el.classList.remove('selected');
        }
      })
      .on('stop', ({ inst }) => {
        inst.keepSelection();
      });
  }, []);
  return (
    <Container className={`box-wrapper`}>
      {Array.apply(null, { length: 100 }).map(() => (
        <Box />
      ))}
    </Container>
  );
}
