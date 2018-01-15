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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import {Observable} from 'rxjs/Observable';

const HOURS_IN_SEC = 3600;
const MINUTE_IN_SEC = 60;

export default class TimeDuration extends Component {
  static propTypes = {
    startTime: PropTypes.number
  };

  componentWillMount() {
    this.calculateDuration();
    this.interval$ = Observable.interval(1000)
      .subscribe(this.calculateDuration.bind(this));
  }

  componentWillUnmount() {
    this.interval$.unsubscribe();
  }

  calculateDuration() {
    let now = new Date().getTime() / 1000;
    let duration = now - this.props.startTime;

    let hours = Math.floor(duration / HOURS_IN_SEC);
    let minutes = Math.floor((duration % HOURS_IN_SEC) / MINUTE_IN_SEC);
    let seconds = Math.floor(duration % 60);

    if (hours < 10) {
      hours = `0${hours}`;
    }

    if (minutes < 10) {
      minutes = `0${minutes}`;
    }

    if (seconds < 10) {
      seconds = `0${seconds}`;
    }

    let durationDisplay = `${hours}:${minutes}:${seconds}`;

    this.setState({
      duration: durationDisplay
    });
  }

  state = {
    duration: '00:00:00'
  };

  render() {
    return (
      <span>
        {this.state.duration}
      </span>
    );
  }
}
