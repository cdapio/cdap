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

import React, { useState, useEffect } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import isEqual from 'lodash/isEqual';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Checkbox from '@material-ui/core/Checkbox';
import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';
import { IWidgetProps } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import OutlinedSelect from 'components/OutlinedSelect';

const styles = (theme): StyleRules => {
  return {
    multiCheckboxesContainer: {
      padding: '10px',
    },
    emptyMessageContainer: {
      backgroundColor: 'initial',
      padding: 'initial',
    },
    emptyMessage: {
      marginBottom: 0,
    },
    checkboxesGroup: {
      display: 'flex',
      flexDirection: 'column',
    },
    subtitle: {
      color: theme.palette.blue[40],
      fontSize: '13px',
      fontWeight: 500,
      marginTop: '15px',
      marginBottom: '5px',
    },
    labelStyles: {
      // Styling stage name label so that for longer stage names,
      // it doesnt overflow - we just break to new line as we have enough
      // vertical real estate; we don't have to truncate.
      // Revisit this later when styling to think about adding reusable Tooltip
      // component
      whiteSpace: 'normal',
      wordBreak: 'break-all',
    },
    textWarning: {
      marginBottom: '5px',
    },
    checkbox: {
      width: '45%',
      display: 'inline-flex',
    },
  };
};

interface IJoinTypeWidgetProps extends IWidgetProps<null>, WithStyles<typeof styles> {}

const DROP_DOWN_OPTIONS: string[] = ['Inner', 'Outer'];

const JoinTypeWidgetView: React.FC<IJoinTypeWidgetProps> = ({
  value,
  extraConfig,
  onChange,
  disabled,
  classes,
}) => {
  const [joinType, setJoinType] = useState(DROP_DOWN_OPTIONS[1]);
  const [selectedCount, setSelectedCount] = useState(0);
  const [inputs, setInputs] = useState([]);
  const inputSchema = objectQuery(extraConfig, 'inputSchema');

  const formatOutput = () => {
    const outputArr = inputs.filter((schema) => schema.selected).map((schema) => schema.name);
    onChange(outputArr.join(','));
    setSelectedCount(outputArr.length);
  };

  const joinTypeChange = (val) => {
    setJoinType(val);
    setInputs(
      inputSchema.map((input) => {
        return { name: input.name, selected: val === 'Inner' };
      })
    );
  };

  const checkBoxChange = (event) => {
    setInputs(
      inputs.map((input, i) => {
        if (`${i}-${input.name}` === event.target.value) {
          input.selected = !input.selected;
        }
        return input;
      })
    );
  };

  useEffect(() => {
    if (!value) {
      setInputs(
        inputSchema.map((input) => {
          return { name: input.name, selected: false };
        })
      );
      return;
    }
    const initialModel = value
      .toString()
      .split(',')
      .map((input) => input.trim());
    if (isEqual(initialModel, inputSchema.map((schema) => schema.name.trim()))) {
      setJoinType('Inner');
      setInputs(
        inputSchema.map((input) => {
          return { name: input.name, selected: true };
        })
      );
    } else {
      setJoinType('Outer');
      setInputs(
        inputSchema.map((input) => {
          return {
            name: input.name,
            selected: initialModel.indexOf(input.name) !== -1 ? true : false,
          };
        })
      );
    }
  }, []);

  useEffect(
    () => {
      formatOutput();
    },
    [inputs]
  );

  return (
    <div>
      <If condition={inputs.length > 0}>
        <div className={classes.multiCheckboxesContainer}>
          <OutlinedSelect
            options={DROP_DOWN_OPTIONS}
            value={joinType}
            onChange={joinTypeChange}
            disabled={disabled}
          />
          <If condition={joinType === 'Outer'}>
            <div className={classes.checkboxesGroup}>
              <div className={classes.subtitle}>Required Inputs</div>
              <If condition={selectedCount === inputs.length}>
                <div className="text-warning">
                  <span>Setting all stages as required inputs will be treated as Inner Join.</span>
                </div>
              </If>
              <div>
                {inputs.map((input, i) => {
                  return (
                    <FormControlLabel
                      className={classes.checkbox}
                      key={`${i}-${input.name}`}
                      disabled={disabled}
                      control={
                        <Checkbox
                          checked={input.selected}
                          value={`${i}-${input.name}`}
                          color="primary"
                          onChange={checkBoxChange}
                        />
                      }
                      label={<span className={classes.labelStyles}>{input.name}</span>}
                    />
                  );
                })}
              </div>
            </div>
          </If>
        </div>
      </If>
      <If condition={inputs.length === 0}>
        <div className={classes.emptyMessageContainer}>
          <h4 className={classes.emptyMessage}>No input stages</h4>
        </div>
      </If>
    </div>
  );
};
const StyledJoinTypeWidget = withStyles(styles)(JoinTypeWidgetView);

export default function JoinTypeWidget(props: IJoinTypeWidgetProps) {
  return (
    <ThemeWrapper>
      <StyledJoinTypeWidget {...props} />
    </ThemeWrapper>
  );
}

(JoinTypeWidget as any).propTypes = WIDGET_PROPTYPES;
