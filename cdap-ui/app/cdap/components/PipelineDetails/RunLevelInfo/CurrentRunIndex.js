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
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import {reverseArrayWithoutMutating, objectQuery} from 'services/helpers';
import findIndex from 'lodash/findIndex';
import {setCurrentRunId} from 'components/PipelineDetails/store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.RunLevel';

const mapStateToProps = (state) => {
  return {
    runs: state.runs,
    currentRun: state.currentRun
  };
};

const CurrentRunIndex = ({runs, currentRun}) => {
  let reversedRuns = reverseArrayWithoutMutating(runs);
  let currentRunIndex = findIndex(reversedRuns, {runid: objectQuery(currentRun, 'runid')});

  if (!reversedRuns || currentRunIndex === -1) {
    return (
      <div className="run-number-container run-info-container">
        <h4 className="run-number">
          {T.translate(`${PREFIX}.noRuns`)}
        </h4>
        <div className="run-number-switches">
          <button disabled>
            <IconSVG name="icon-caret-left" />
          </button>
          <button disabled>
            <IconSVG name="icon-caret-right" />
          </button>
        </div>
      </div>
    );
  }

  let previousRunId, nextRunId;
  if (currentRunIndex > 0) {
    previousRunId = reversedRuns[currentRunIndex - 1].runid;
  }
  if (currentRunIndex < reversedRuns.length - 1) {
    nextRunId =  reversedRuns[currentRunIndex + 1].runid;
  }

  return (
    <div className="run-number-container run-info-container">
      <h4 className="run-number">
        {T.translate(`${PREFIX}.currentRunIndex`, {currentRunIndex: currentRunIndex + 1, numRuns: runs.length})}
      </h4>
      <div className="run-number-switches">
        <button
          disabled={!previousRunId}
          onClick={setCurrentRunId.bind(null, previousRunId)}
        >
          <IconSVG name="icon-caret-left" />
        </button>
        <button
          disabled={!nextRunId}
          onClick={setCurrentRunId.bind(null, nextRunId)}
        >
          <IconSVG name="icon-caret-right" />
        </button>
      </div>
    </div>
  );
};

CurrentRunIndex.propTypes = {
  runs: PropTypes.array,
  currentRun: PropTypes.object
};

const ConnectedCurrentRunIndex = connect(mapStateToProps)(CurrentRunIndex);
export default ConnectedCurrentRunIndex;
