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
import TABLES from './tablesDefinition';
import FormControl from '@material-ui/core/FormControl';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import InputLabel from '@material-ui/core/InputLabel';
import CheckCircle from '@material-ui/icons/CheckCircle';
import Warning from '@material-ui/icons/Warning';
import Error from '@material-ui/icons/Error';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    grid: {
      marginTop: '15px',
    },
    gridRow: {
      display: 'grid',
      gridTemplateColumns: '50px 1fr 1fr 1fr 1fr 75px',
      alignItems: 'center',
      borderBottom: `1px solid ${theme.palette.grey[400]}`,
      '& > div': {
        padding: '7px 15px',
      },
    },
    gridHeader: {
      fontWeight: 600,
      borderBottom: `2px solid ${theme.palette.grey[400]}`,
    },
    error: {
      backgroundColor: theme.palette.red[300],
    },
    warning: {
      backgroundColor: theme.palette.yellow[200],
    },
  };
};

interface IProps extends WithStyles<typeof styles> {}

const checkCircleStyles = (theme): StyleRules => {
  return {
    root: {
      color: theme.palette.green[100],
    },
  };
};
const StyledCheckCircle = withStyles(checkCircleStyles)(CheckCircle);

const errorStyles = (theme): StyleRules => {
  return {
    root: {
      color: theme.palette.red[100],
    },
  };
};
const StyledError = withStyles(errorStyles)(Error);

const warningStyles = (theme): StyleRules => {
  return {
    root: {
      color: theme.palette.yellow[50],
    },
  };
};
const StyledWarning = withStyles(warningStyles)(Warning);

const ICON_MAP = {
  default: StyledCheckCircle,
  ERROR: StyledError,
  WARNING: StyledWarning,
};

const TargetTypesSelect = ({ types }) => {
  if (!types || types.length === 0) {
    return null;
  }
  if (types.length === 1) {
    return types[0];
  }

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

const TransformFunction = () => {
  const [fn, setFn] = React.useState();

  function handleChange(e) {
    setFn(e.target.value);
  }

  const fnList = [
    'Mask',
    'Encrypt',
    'Truncate',
    'Drop',
    'Uppercase',
    'Lowercase',
    'PII Filter',
    'Custom',
  ];

  return (
    <Select value={fn} onChange={handleChange}>
      {fnList.map((func) => {
        return (
          <MenuItem key={func} value={func}>
            {func}
          </MenuItem>
        );
      })}
    </Select>
  );
};

const SchemaAssessmentView: React.SFC<IProps> = ({ classes }) => {
  const [table, setTable] = React.useState(0);

  function handleOnChange(e) {
    const index = e.target.value;
    setTable(index);
  }

  return (
    <div>
      <div>
        <FormControl>
          <InputLabel>Table</InputLabel>
          <Select value={table} onChange={handleOnChange}>
            {TABLES.map((tableInfo, i) => {
              return (
                <MenuItem key={tableInfo.name} value={i}>
                  {tableInfo.name}
                </MenuItem>
              );
            })}
          </Select>
        </FormControl>
      </div>

      <div>
        <div className={classes.grid}>
          <div className={classes.gridHeader}>
            <div className={classes.gridRow}>
              <div />
              <div>Field name</div>
              <div>Source Type</div>
              <div>Target Type</div>
              <div>Transform</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {TABLES[table].columns.map((column) => {
              const IconComp =
                typeof column.issue === 'string' ? ICON_MAP[column.issue] : ICON_MAP.default;

              return (
                <div
                  key={column.field}
                  className={classnames(classes.gridRow, {
                    [classes.error]: column.issue === 'ERROR',
                    [classes.warning]: column.issue === 'WARNING',
                  })}
                >
                  <div>
                    <IconComp />
                  </div>
                  <div>{column.field}</div>
                  <div>{column.sourceType}</div>
                  <div>
                    <TargetTypesSelect types={column.targetTypes} />
                  </div>
                  <div>
                    <TransformFunction />
                  </div>
                  <div />
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const StyledSchemaAssessment = withStyles(styles)(SchemaAssessmentView);
const SchemaAssessment = transfersCreateConnect(StyledSchemaAssessment);
export default SchemaAssessment;
