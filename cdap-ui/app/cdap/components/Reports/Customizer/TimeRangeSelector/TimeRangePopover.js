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
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';
import TimeRangePicker from 'components/TimeRangePicker';
import {connect} from 'react-redux';
import {ReportsActions} from 'components/Reports/store/ReportsStore';
import T from 'i18n-react';

const PREFIX = 'features.Reports.Customizer.TimeRangeSelector';

class TimeRangePopoverView extends Component {
  static propTypes = {
    onApply: PropTypes.func,
    start: PropTypes.number,
    end: PropTypes.number,
    selection: PropTypes.string
  };

  state = {
    selection: this.props.selection,
    start: this.props.start,
    end: this.props.end
  };

  componentWillReceiveProps(nextProps) {
    this.setState({
      selection: nextProps.selection,
      start: nextProps.start,
      end: nextProps.end
    });
  }

  changeSelection = (selection) => {
    this.setState({ selection });
  };

  onCustomRangeChange = ({start, end}) => {
    this.setState({
      start,
      end
    });
  };

  apply = () => {
    let selection = this.state.selection;
    let start = null,
        end = null;

    if (selection === 'custom') {
      start = this.state.start;
      end = this.state.end;
    }

    this.props.onApply({
      selection,
      start,
      end
    });

    // to close popover
    document.body.click();
  };

  renderCustomRange = () => {
    if (this.state.selection !== 'custom') { return null; }

    return (
      <div className="custom-range-container">
        <TimeRangePicker
          onChange={this.onCustomRangeChange}
          start={this.state.start}
          end={this.state.end}
        />
      </div>
    );
  };

  renderApplyButton = () => {
    let disabled = !this.state.selection ||
                    (
                      this.state.selection === 'custom' &&
                      (!this.state.start || !this.state.end)
                    );

    return (
      <div className="apply-button">
        <button
          className="btn btn-primary"
          disabled={disabled}
          onClick={this.apply}
        >
          {T.translate('commons.apply')}
        </button>
      </div>
    );
  };

  render() {
    return (
      <Popover
        target={() => <IconSVG name="icon-calendar" />}
        className="time-range-popover"
        placement="bottom"
        bubbleEvent={false}
        enableInteractionInPopover={true}
        injectOnToggle={true}
      >
        <div className="title">
          {T.translate(`${PREFIX}.labelWithColon`)}
        </div>

        <div className="options">
          <div
            className="option"
            onClick={this.changeSelection.bind(this, 'last30')}
          >
            <IconSVG name={this.state.selection === 'last30' ? 'icon-circle' : 'icon-circle-o'} />
            {T.translate(`${PREFIX}.last30Min`)}
          </div>

          <div
            className="option"
            onClick={this.changeSelection.bind(this, 'lastHour')}
          >
            <IconSVG name={this.state.selection === 'lastHour' ? 'icon-circle' : 'icon-circle-o'} />
            {T.translate(`${PREFIX}.lastHour`)}
          </div>

          <div
            className="option"
            onClick={this.changeSelection.bind(this, 'custom')}
          >
            <IconSVG name={this.state.selection === 'custom' ? 'icon-circle' : 'icon-circle-o'} />
            {T.translate(`${PREFIX}.customRange`)}
          </div>
        </div>

        {this.renderCustomRange()}

        {this.renderApplyButton()}
      </Popover>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    start: state.timeRange.start,
    end: state.timeRange.end,
    selection: state.timeRange.selection
  };
};

const mapDispatch = (dispatch) => {
  return {
    onApply: (payload) => {
      dispatch({
        type: ReportsActions.setTimeRange,
        payload
      });
    }
  };
};

const TimeRangePopover = connect(
  mapStateToProps,
  mapDispatch
)(TimeRangePopoverView);

export default TimeRangePopover;
