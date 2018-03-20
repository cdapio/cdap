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

import React from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import Duration from 'components/Duration';
import {humanReadableDuration} from 'services/helpers';

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun
  };
};

const RunDuration = ({currentRun}) => {
  let DurationComp;
  if (currentRun && currentRun.start) {
    if (currentRun.end) {
      DurationComp = (
        <span>
          {`${humanReadableDuration(currentRun.end - currentRun.start)}`}
        </span>
      );
    } else {
      DurationComp = (
        <Duration
          targetTime={currentRun.start}
          isMillisecond={false}
          showFullDuration={true}
        />
      );
    }
  }
  return (
    <div className="run-info-container">
      <div>
        <strong>Duration</strong>
      </div>
      <span>
        {
          currentRun && currentRun.start ?
            DurationComp
          :
            '--'
        }
      </span>
    </div>
  );
};

RunDuration.propTypes = {
  currentRun: PropTypes.object
};

const ConnectedRunDuration = connect(mapStateToProps)(RunDuration);
export default ConnectedRunDuration;
