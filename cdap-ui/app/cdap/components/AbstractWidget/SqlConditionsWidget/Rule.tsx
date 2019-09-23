/*
 * Copyright © 2019 Cask Data, Inc.
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
 */

import React, { useState } from 'react';
import Paper from '@material-ui/core/Paper';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import IconButton from '@material-ui/core/IconButton';
import AddIcon from '@material-ui/icons/AddBox';
import CancelIcon from '@material-ui/icons/Cancel';
import If from 'components/If';
import { IInputSchema, IRule } from 'components/AbstractWidget/SqlConditionsWidget';
import OutlinedSelect from 'components/OutlinedSelect';

const styles = (theme): StyleRules => {
  return {
    root: {
      marginBottom: '15px',
    },
    ruleContainer: {
      display: 'flex',
      flexAlign: 'row',
      width: '100%',
    },
    rule: {
      position: 'relative',
      width: '100%',
      padding: '15px',
    },
    actionButtons: {
      display: 'flex',
      alignItems: 'flex-end',
      width: '50px',
      paddingLeft: '10px',
    },
    stageRow: {
      display: 'grid',
      gridTemplateColumns: '45% 45% 10%',
      marginBottom: '5px',
    },
    tableCell: {
      display: 'inherit',
      alignItems: 'center',
      wordBreak: 'break-all',
    },
    fieldEquality: {
      color: theme.palette.white[50],
      width: '20px',
      height: '20px',
      textAlign: 'center',
      backgroundColor: theme.palette.grey[100],
      marginLeft: '5px',
    },
    deleteButton: {
      position: 'absolute',
      top: '-16px',
      right: '-16px',
      color: theme.palette.red[200],
    },
    andBlock: {
      borderRadius: '0',
      color: theme.palette.white[50],
      backgroundColor: theme.palette.grey[100],
      padding: '5px',
      '&:hover': { textDecoration: 'none' },
    },
    errorText: {
      marginTop: '5px',
      color: theme.palette.red[50],
    },
  };
};

interface IRuleProps extends WithStyles<typeof styles> {
  rule: IRule;
  inputSchema: IInputSchema;
  disabled: boolean;
  ruleIdx: number;
  rulesCount: number;
  addRule: () => void;
  updateRule: (rule: IRule) => void;
  deleteRule: (rule: number) => void;
  error: string;
}

const Rule: React.FC<IRuleProps> = ({
  rule,
  inputSchema,
  rulesCount,
  ruleIdx,
  disabled,
  addRule,
  updateRule,
  deleteRule,
  classes,
  error,
}) => {
  const [hovered, setHovered] = useState(false);
  const last = ruleIdx === rulesCount - 1;
  const fieldChange = (stageIdx, val) => {
    const newRule = rule.map((row, i) => {
      if (i === stageIdx) {
        return { ...row, fieldName: val };
      }
      return row;
    });
    updateRule(newRule);
  };
  return (
    <div className={classes.root}>
      <div
        className={classes.ruleContainer}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >
        <Paper className={classes.rule}>
          <If condition={hovered && rulesCount !== 1 && !disabled}>
            <IconButton
              size="small"
              className={classes.deleteButton}
              onClick={() => deleteRule(ruleIdx)}
            >
              <CancelIcon fontSize="large" />
            </IconButton>
          </If>
          {rule.map((stage, i) => (
            <div key={`${i}-${stage.stageName}`} className={classes.stageRow}>
              <div className={classes.tableCell}>{stage.stageName}</div>
              <div className={classes.tableCell}>
                <OutlinedSelect
                  options={inputSchema[stage.stageName]}
                  disabled={disabled}
                  value={stage.fieldName}
                  onChange={(val) => fieldChange(i, val)}
                />
              </div>
              <If condition={i !== rule.length - 1}>
                <div className={classes.tableCell}>
                  <span className={classes.fieldEquality}>=</span>
                </div>
              </If>
            </div>
          ))}
        </Paper>
        <div className={classes.actionButtons}>
          <If condition={!last}>
            <span className={classes.andBlock}>AND</span>
          </If>
          <If condition={last}>
            <IconButton disabled={disabled} size="small" onClick={addRule}>
              <AddIcon fontSize="large" />
            </IconButton>
          </If>
        </div>
      </div>
      <If condition={error !== ''}>
        <div className={classes.errorText}>{error}</div>
      </If>
    </div>
  );
};

export default withStyles(styles)(Rule);
