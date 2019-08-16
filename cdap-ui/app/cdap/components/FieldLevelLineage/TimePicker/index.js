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
import { connect } from 'react-redux';
import { ButtonDropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import { setTimeRange, setCustomTimeRange } from 'components/FieldLevelLineage/store/ActionCreator';
import { TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import ExpandableTimeRange from 'components/TimeRangePicker/ExpandableTimeRange';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.TimeRangeOptions';

export class TimePickerView extends Component {
  static propTypes = {
    selections: PropTypes.string,
    start: PropTypes.number,
    end: PropTypes.number,
  };

  state = {
    dropdownOpen: false,
  };

  toggle = () => {
    this.setState({
      dropdownOpen: !this.state.dropdownOpen,
    });
  };

  onDone = ({ start, end }) => {
    setCustomTimeRange({ start, end });
  };

  renderCustomTimeRange() {
    if (this.props.selections !== TIME_OPTIONS[0]) {
      return null;
    }

    return (
      <div className="custom-time-range-container">
        <ExpandableTimeRange
          onDone={this.onDone}
          inSeconds={true}
          start={this.props.start}
          end={this.props.end}
        />
      </div>
    );
  }

  render() {
    return (
      <div className="time-picker-dropdown" data-cy="fll-time-picker">
        <ButtonDropdown isOpen={this.state.dropdownOpen} toggle={this.toggle}>
          <DropdownToggle caret>
            <h5>{T.translate(`${PREFIX}.${this.props.selections}`)}</h5>
          </DropdownToggle>

          <DropdownMenu>
            {TIME_OPTIONS.map((option) => {
              return (
                <DropdownItem
                  data-cy={option}
                  key={option}
                  onClick={setTimeRange.bind(null, option)}
                >
                  {T.translate(`${PREFIX}.${option}`)}
                </DropdownItem>
              );
            })}
          </DropdownMenu>
        </ButtonDropdown>

        {this.renderCustomTimeRange()}
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    selections: state.lineage.timeSelection,
    start: state.customTime.start,
    end: state.customTime.end,
  };
};

const TimePicker = connect(mapStateToProps)(TimePickerView);

export default TimePicker;
