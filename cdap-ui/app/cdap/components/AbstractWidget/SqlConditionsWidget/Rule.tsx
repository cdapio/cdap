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
import PropTypes from 'prop-types';
import classnames from 'classnames';

import Paper from '@material-ui/core/Paper';
import { WithStyles } from '@material-ui/styles';
import { withStyles } from '@material-ui/core';
import IconButton from '@material-ui/core/IconButton';

import AddIcon from '@material-ui/icons/AddBox';
import CancelIcon from '@material-ui/icons/Cancel';

import Select from 'components/AbstractWidget/SelectWidget';
import If from 'components/If';

import { IInputSchema, IRule } from 'components/AbstractWidget//SqlConditionsWidget';

const styles = (theme) => {
  return {
    root: {
      display: 'flex',
      flexAlign: 'row',
      width: '100%',
    },
    rule: {
      position: 'relative' as 'relative',
      width: '100%',
      marginBottom: '15px',
      padding: '15px',
    },
    actionButtons: {
      display: 'flex',
      alignItems: 'flex-end',
      marginBottom: '15px',
      width: '50px',
      paddingLeft: '10px',
    },
    stageRow: {
      display: 'grid',
      gridTemplateColumns: '45% 45% 10%',
      marginBottom: '5px',
    },
    stageName: {
      display: 'flex',
      alignItems: 'center',
    },
    fieldEquality: {
      display: 'flex',
      alignItems: 'center',
      '& span': {
        color: 'white',
        width: '20px',
        height: '20px',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'baseline',
        backgroundColor: theme.palette.grey[100],
        marginLeft: '5px',
      },
    },
    deleteButton: {
      position: 'absolute' as 'absolute',
      top: '-16px',
      right: '-16px',
      color: theme.palette.red[200],
    },
    andButton: {
      borderRadius: '0',
      color: 'white',
      backgroundColor: theme.palette.grey[100],
      padding: '5px',
      '&:hover': { textDecoration: 'none' },
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
  updateRule: (arg0: IRule) => void;
  deleteRule: (arg0: number) => void;
}

function Rule({
  rule,
  inputSchema,
  rulesCount,
  ruleIdx,
  disabled,
  addRule,
  updateRule,
  deleteRule,
  classes,
}: IRuleProps) {
  const [hovered, setHovered] = useState(false);
  const last = ruleIdx === rulesCount - 1;
  const fieldChange = (stageIdx, event) => {
    const newRule = rule.map((row, i) => {
      if (i === stageIdx) {
        return { ...row, fieldName: event.target.value };
      }
      return row;
    });
    updateRule(newRule);
  };
  return (
    <div
      className={classes.root}
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
          <div className={classes.stageRow}>
            <div className={classes.stageName}>{stage.stageName}</div>
            <div>
              <Select
                widgetProps={{
                  values: inputSchema[stage.stageName],
                }}
                value={stage.fieldName}
                onChange={(e) => fieldChange(i, e)}
              />
            </div>
            <If condition={i !== rule.length - 1}>
              <div className={classes.fieldEquality}>
                <span>=</span>
              </div>
            </If>
          </div>
        ))}
      </Paper>
      <div className={classes.actionButtons}>
        <If condition={!last}>
          <span className={classnames('btn', 'btn-sm', classes.andButton)}>AND</span>
        </If>
        <If condition={last}>
          <IconButton size="small" onClick={addRule}>
            <AddIcon fontSize="large" />
          </IconButton>
        </If>
      </div>
    </div>
  );
}

(Rule as any).propTypes = {
  classes: PropTypes.object.isRequired,
  last: PropTypes.bool,
  disabled: PropTypes.bool,
  ruleIdx: PropTypes.number,
  rulesCount: PropTypes.number,
  inputSchema: PropTypes.object,
  rule: PropTypes.array,
  addRule: PropTypes.func,
  updateRule: PropTypes.func,
  deleteRule: PropTypes.func,
};
const StyledRule=withStyles(styles)(Rule);
export default React.memo(StyledRule);
