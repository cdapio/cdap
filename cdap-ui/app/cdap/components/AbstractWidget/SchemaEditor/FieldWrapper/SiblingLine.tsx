/*
 * Copyright © 2020 Cask Data, Inc.
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
import classnames from 'classnames';
const widthOfSiblingLines = 10;
const borderSizeOfSiblingLines = 2;
import makeStyles from '@material-ui/core/styles/makeStyles';
import { blue, red } from 'components/ThemeWrapper/colors';
import {
  INDENTATION_SPACING,
  rowHeight,
  rowMarginTop,
} from 'components/AbstractWidget/SchemaEditor/FieldWrapper/FieldWrapperConstants';

const useStyles = makeStyles({
  root: {
    position: 'absolute',
    height: `${rowHeight + rowMarginTop * 2 + 2}px`,
    width: `${widthOfSiblingLines}px`,
    borderLeft: `${borderSizeOfSiblingLines}px solid rgba(0, 0, 0, 0.2)`,
    left: (props) => (props as any).index * -1 * INDENTATION_SPACING,
  },
  innerMostSiblingConnector: {
    '&:after': {
      position: 'absolute',
      height: '2px',
      width: `${INDENTATION_SPACING - borderSizeOfSiblingLines}px`,
      left: '0px',
      content: '""',
      borderTop: '2px solid rgba(0, 0, 0, 0.2)',
      top: `${rowHeight / 2}px`,
    },
  },
  highlight: {
    borderLeftColor: `${blue[300]}`,
    '&:after': {
      borderTopColor: `${blue[300]}`,
    },
  },
  errorHighlight: {
    borderLeftColor: `${red[100]}`, // TODO: should be able to use from theme.
    '&:after': {
      borderTopColor: `${red[100]}`,
    },
  },
});

const SiblingLine = ({ id, index, activeParent, setActiveParent, ancestors, error = false }) => {
  const classes = useStyles({ index: ancestors.length - 1 - index });
  if (index + 1 === ancestors.length - 1) {
    return (
      <div
        onMouseEnter={() => setActiveParent(id)}
        onMouseLeave={() => setActiveParent(null)}
        className={classnames(`${classes.root} ${classes.innerMostSiblingConnector}`, {
          [classes.highlight]: id === activeParent,
          [classes.errorHighlight]: error,
        })}
        key={id}
        data-ancestor-id={id}
      />
    );
  }
  return (
    <div
      onMouseEnter={() => setActiveParent(id)}
      onMouseLeave={() => setActiveParent(null)}
      className={classnames(classes.root, {
        [classes.highlight]: id === activeParent,
        [classes.errorHighlight]: error,
      })}
      data-ancestor-id={id}
      key={index}
    />
  );
};
export { SiblingLine };
