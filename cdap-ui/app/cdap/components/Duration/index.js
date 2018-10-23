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
import moment from 'moment';
import {
  humanReadableDuration,
  ONE_SECOND_MS,
  ONE_MIN_SECONDS,
  ONE_HOUR_SECONDS,
} from 'services/helpers';

export default class Duration extends Component {
  static propTypes = {
    targetTime: PropTypes.number,
    isMillisecond: PropTypes.bool,
    showFullDuration: PropTypes.bool,
  };

  static defaultProps = {
    isMillisecond: true,
    showFullDuration: false,
  };

  componentWillMount() {
    this.calculateTime();
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.targetTime !== this.props.targetTime) {
      this.stopCounter();
    }

    this.calculateTime(nextProps.targetTime);
  }

  componentWillUnmount() {
    this.stopCounter();
  }

  state = {
    displayDuration: null,
  };

  stopCounter() {
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
  }

  calculateTime(newTime = this.props.targetTime) {
    if (!newTime) {
      return;
    }

    let targetTime = newTime;

    if (!this.props.isMillisecond) {
      targetTime *= ONE_SECOND_MS;
    }

    if (this.props.showFullDuration) {
      let duration = new Date().valueOf() - targetTime;
      this.setState(
        {
          displayDuration: humanReadableDuration((duration /= ONE_SECOND_MS)),
        },
        this.calculateTimeCallback.bind(this, duration)
      );
    } else {
      let duration = targetTime - new Date().valueOf();
      let isPast = duration < 0;

      this.setState(
        {
          displayDuration: moment.duration(duration).humanize(isPast),
        },
        this.calculateTimeCallback.bind(this, duration)
      );
    }
  }

  calculateTimeCallback = (duration) => {
    let delay = ONE_SECOND_MS;

    if (!this.props.showFullDuration) {
      let absDuration = Math.abs(duration);

      if (absDuration > ONE_HOUR_SECONDS) {
        delay = 15 * ONE_MIN_SECONDS;
      } else if (absDuration > 5 * ONE_MIN_SECONDS) {
        delay = ONE_MIN_SECONDS;
      } else if (absDuration > 2 * ONE_MIN_SECONDS) {
        delay = 15 * ONE_SECOND_MS;
      }
    }

    this.timeout = setTimeout(() => {
      this.calculateTime();
    }, delay);
  };

  render() {
    if (!this.props.targetTime) {
      return <span className="duration-display">--</span>;
    }

    return <span className="duration-display">{this.state.displayDuration}</span>;
  }
}
