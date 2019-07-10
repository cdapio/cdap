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

import React, {useState, useEffect} from 'react';
import PropTypes from 'prop-types';
import Select from 'components/AbstractWidget/SelectWidget';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Checkbox from '@material-ui/core/Checkbox';
import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';

interface IJoinTypeWidgetProps {
  value: string;
  inputSchema: Array<{name: string}>;
  onChange: (arg0: string) => void;
}

const DROP_DOWN_OPTIONS: Array<string> = ['Inner', 'Outer'];

export default function JoinTypeWidget({
  value = '',
  inputSchema,
  onChange
}: IJoinTypeWidgetProps) {
  const [joinType, setJoinType] = useState(DROP_DOWN_OPTIONS[1]);
  const [selectedCount, setSelectedCount] = useState(0);
  const [inputs, setInputs] = useState([]);

  const formatOutput = () => {
    const outputArr = inputs.filter(schema => schema.selected).map(schema => schema.name);
    onChange(outputArr.join(','));
    setSelectedCount(outputArr.length);
  };

  const joinTypeChange = event => {
    setJoinType(event.target.value);
    setInputs(
      inputSchema.map(input => {
        return {name: input.name, selected: event.target.value === 'Inner'};
      })
    );
  };

  const checkBoxChange = event => {
    setInputs(
      inputs.map(input => {
        if (input.name === event.target.value) {
          input.selected = !input.selected;
        }
        return input;
      })
    );
  };

  useEffect(() => {
    const initialModel = value.split(',').map(input => input.trim());
    if (!value) {
      setInputs(
        inputSchema.map(input => {
          return {name: input.name, selected: false};
        })
      );
    }
    if (initialModel.length === inputSchema.length) {
      setJoinType('Inner');
      setInputs(
        inputSchema.map(input => {
          return {name: input.name, selected: true};
        })
      );
    } else {
      setJoinType('Outer');
      setInputs(
        inputSchema.map(input => {
          return {
            name: input.name,
            selected: initialModel.indexOf(input.name) !== -1 ? true : false
          };
        })
      );
    }
  }, []);

  useEffect(() => {
    formatOutput();
  }, [inputs]);

  return (
    <ThemeWrapper>
      {inputs.length > 0 ? (
        <div className='multi-checkboxes-container'>
          <Select
            widgetProps={{ values: DROP_DOWN_OPTIONS }}
            value={joinType}
            onChange={joinTypeChange}
          />
          <If condition={joinType === 'Outer'}>
            <div className='checkboxes-group'>
              <div className='subtitle'>Required Inputs</div>
              <If condition={selectedCount === inputs.length}>
                <div className='text-warning'>
                  <span>Setting all stages as required inputs will be treated as Inner Join.</span>
                </div>
              </If>
              <div className='row checkboxes'>
                {inputs &&
                  inputs.map(input => {
                    return (
                      <div className='col-xs-6 clearfix'>
                        <FormControlLabel
                          control={
                            <Checkbox
                              checked={input.selected}
                              value={input.name}
                              color='primary'
                              onChange={checkBoxChange}
                              className='checkbox'
                            />
                          }
                          label={input.name}
                        />
                      </div>
                    );
                  })}
              </div>
            </div>
          </If>
        </div>
      ) : (
        <div className='empty'>
          <h4>No input stages</h4>
        </div>
      )}
    </ThemeWrapper>
  );

}

(JoinTypeWidget as any).propTypes = {
  value: PropTypes.string,
  inputSchema: PropTypes.object,
  onChange: PropTypes.func
};
