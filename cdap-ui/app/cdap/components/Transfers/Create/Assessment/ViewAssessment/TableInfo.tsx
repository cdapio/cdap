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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import { Button, Select, MenuItem } from '@material-ui/core';
import IconSVG from 'components/IconSVG';
import { BQ_TYPES } from './dataTypes';

const styles = (theme): StyleRules => {
  return {
    root: {
      position: 'absolute',
      right: 0,
      top: '125px',
      width: '800px',
      height: 'calc(100vh - 180px)',
      backgroundColor: theme.palette.white[50],
      border: `2px solid ${theme.palette.grey[200]}`,
      borderRight: 0,
      zIndex: 1,
      padding: '15px 25px',
      boxShadow: '-4px 0 15px 0px rgba(0, 0, 0, 0.4)',
    },
    header: {
      display: 'grid',
      gridTemplateColumns: '1fr 100px',
      marginBottom: '10px',
    },
    close: {
      position: 'absolute',
      top: '15px',
      right: '25px',
      cursor: 'pointer',
    },
  };
};

const TargetTypesSelect = () => {
  const types = Object.keys(BQ_TYPES).map((type) => BQ_TYPES[type]);

  const [selection, setSelection] = React.useState(types[0]);
  function handleChange(e) {
    setSelection(e.target.value);
  }

  return (
    <Select value={selection} onChange={handleChange}>
      {types.map((type) => {
        return (
          <MenuItem key={type} value={type}>
            {type}
          </MenuItem>
        );
      })}
    </Select>
  );
};

interface ITableInfo extends WithStyles<typeof styles> {
  activeTable?: string;
  onClose: () => void;
}

const TableInfoView: React.SFC<ITableInfo> = ({ activeTable, classes, onClose }) => {
  if (!activeTable) {
    return null;
  }

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <div>
          <div>Mapping details</div>
          <h3>{activeTable}</h3>
          <div>Review source and target compatibility issues around data type</div>
        </div>

        <div>
          <Button color="primary" variant="contained" onClick={onClose}>
            Save
          </Button>

          <IconSVG name="icon-close" className={classes.close} onClick={onClose} />
        </div>
      </div>

      <div>
        <div>56 Columns</div>

        <table className="table">
          <thead>
            <tr>
              <th>Column name</th>
              <th>Data type supported</th>
              <th>Source</th>
              <th>Target</th>
              <th>Suggestion</th>
            </tr>
          </thead>

          <tbody>
            <tr>
              <td>Column 1</td>
              <td>No</td>
              <td>double</td>
              <td>
                <TargetTypesSelect />
              </td>
              <td>Remove column</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
};

const StyledTableInfo = withStyles(styles)(TableInfoView);
const TableInfo = transfersCreateConnect(StyledTableInfo);
export default TableInfo;
