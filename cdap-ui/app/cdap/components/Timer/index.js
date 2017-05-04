/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import isNil from 'lodash/isNil';
require('./Timer.scss');

export default class Timer extends Component {
  constructor(props) {
    super(props);

    this.state = {
      time: this.props.time || 0
    };
    this.currentTimer = 0;
    this.unmounted = false;
  }

  componentDidMount() {
    this.startTimer();
  }

  componentWillReceiveProps(nextProps) {
    if (!isNil(nextProps.time) && this.props.time !== nextProps.time) {
      clearTimeout(this.currentTimer);
      !this.unmounted && this.setState({time: nextProps.time}, this.startTimer.bind(this));
    }
  }

  componentWillUnmount() {
    if (this.currentTimer) {
      clearTimeout(this.currentTimer);
    }
    this.unmounted = true;
  }
  startTimer() {
    let newTime = this.state.time - 1;
    if (this.unmounted) {
      return;
    }
    !this.unmounted && this.setState({time: newTime});

    if (newTime > 0) {
      this.currentTimer = setTimeout(() => {
        this.startTimer();
      }, 1000);
    } else {
      if (typeof this.props.onDone === 'function') {
        this.props.onDone();
      }
    }
  }

  render() {
    return (
      <span className="timer-countdown">
        {this.state.time}
      </span>
    );
  }
}
Timer.propTypes = {
  time: PropTypes.number,
  onDone: PropTypes.func,
};
