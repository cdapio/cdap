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
import TimeRangePicker from 'components/TimeRangePicker';
import { isDescendant } from 'services/helpers';
import { Observable } from 'rxjs/Observable';
import classnames from 'classnames';

export default class ExpandableTimeRange extends Component {
  static propTypes = {
    onChange: PropTypes.func,
    start: PropTypes.number,
    end: PropTypes.number,
    onDone: PropTypes.func,
    inSeconds: PropTypes.bool,
  };

  static defaultProps = {
    inSeconds: false,
  };

  state = {
    start: this.props.inSeconds ? this.props.start * 1000 : this.props.start,
    end: this.props.inSeconds ? this.props.end * 1000 : this.props.end,
    displayOnly: true,
  };

  onTimeClick = () => {
    this.subscribeClick();
    this.setState({
      displayOnly: false,
    });
  };

  subscribeClick = () => {
    if (this.bodyClick$) {
      return;
    }

    this.bodyClick$ = Observable.fromEvent(document, 'click').subscribe((e) => {
      if (isDescendant(this.timeRangeElem, e.target)) {
        return;
      }

      this.closeTimeRange();
    });
  };

  closeTimeRange = () => {
    this.setState({
      displayOnly: true,
    });

    this.bodyClick$.unsubscribe();
    this.bodyClick$ = null;
  };

  onChange = ({ start, end }) => {
    this.setState({
      start,
      end,
    });
  };

  done = () => {
    if (!this.state.start || !this.state.end) {
      return;
    }

    if (typeof this.props.onDone === 'function') {
      let start = parseInt(this.state.start, 10),
        end = parseInt(this.state.end, 10);

      if (this.props.inSeconds) {
        start = Math.floor(start / 1000);
        end = Math.floor(end / 1000);
      }

      this.props.onDone({
        start,
        end,
      });
    }

    this.closeTimeRange();
  };

  renderDoneButton = () => {
    if (this.state.displayOnly) {
      return null;
    }

    return (
      <div
        className={classnames('done-button text-center', {
          disabled: !this.state.start || !this.state.end,
        })}
        onClick={this.done}
      >
        Done
      </div>
    );
  };

  render() {
    return (
      <div
        className={classnames('expandable-time-range-picker', {
          expanded: !this.state.displayOnly,
        })}
        ref={(ref) => (this.timeRangeElem = ref)}
      >
        <TimeRangePicker
          onChange={this.onChange}
          displayOnly={this.state.displayOnly}
          start={this.state.start}
          end={this.state.end}
          onTimeClick={this.onTimeClick}
        />

        {this.renderDoneButton()}
      </div>
    );
  }
}
