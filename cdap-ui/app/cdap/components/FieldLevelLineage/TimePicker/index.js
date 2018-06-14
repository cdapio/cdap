/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import { ButtonDropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import {setTimeRange} from 'components/FieldLevelLineage/store/ActionCreator';
import {TIME_OPTIONS} from 'components/FieldLevelLineage/store/Store';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.TimeRangeOptions';

class TimePickerView extends Component {
  static propTypes = {
    selections: PropTypes.string
  };

  state = {
    dropdownOpen: false
  };

  toggle = () => {
    this.setState({
      dropdownOpen: !this.state.dropdownOpen
    });
  };

  render() {
    return (
      <div className="time-picker-dropdown">
        <ButtonDropdown
          isOpen={this.state.dropdownOpen}
          toggle={this.toggle}
        >
          <DropdownToggle caret>
            {T.translate(`${PREFIX}.${this.props.selections}`)}
          </DropdownToggle>

          <DropdownMenu>
            {
              TIME_OPTIONS.map((option) => {
                return (
                  <DropdownItem
                    key={option}
                    onClick={setTimeRange.bind(null, option)}
                  >
                    {T.translate(`${PREFIX}.${option}`)}
                  </DropdownItem>
                );
              })
            }
          </DropdownMenu>
        </ButtonDropdown>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    selections: state.lineage.timeSelection
  };
};

const TimePicker = connect(
  mapStateToProps
)(TimePickerView);

export default TimePicker;
