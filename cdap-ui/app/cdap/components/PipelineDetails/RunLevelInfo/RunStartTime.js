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
import moment from 'moment';

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun
  };
};

const RunStartTime = ({currentRun}) => {
  return (
    <div className="run-info-container">
      <div>
        <strong>Start Time</strong>
      </div>
      <span>
        {
          currentRun && currentRun.start ?
            `${moment(currentRun.start * 1000).format('hh:mm:ss a')}`
          :
            '--'
        }
      </span>
    </div>
  );
};

RunStartTime.propTypes = {
  currentRun: PropTypes.object
};

const ConnectedRunStartTime = connect(mapStateToProps)(RunStartTime);
export default ConnectedRunStartTime;
