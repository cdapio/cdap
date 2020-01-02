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

import Proptypes from 'prop-types';
import React from 'react';
import { connect } from 'react-redux';
import isEmpty from 'lodash/isEmpty';
import { getFilteredRuntimeArgs } from 'components/PipelineConfigurations/Store/ActionCreator';
require('./PipelineRuntimeArgsCounter.scss');

const getTotalRuntimeArgsCount = (runtimeArgs) => {
  let count = 0;
  runtimeArgs.pairs.forEach((runtimeArg) => {
    if (!isEmpty(runtimeArg.key)) {
      count += 1;
    }
  });
  return count;
};

function PipelineRuntimeArgsCounter({ runtimeArgs }) {
  let runtimeArgsCount = getTotalRuntimeArgsCount(runtimeArgs);
  if (runtimeArgsCount === 0) {
    return <span className="pipeline-runtime-args-counter">No runtime arguments</span>;
  }
  return (
    <span className="pipeline-runtime-args-counter">
      {`${runtimeArgsCount} runtime argument${runtimeArgsCount === 1 ? '' : 's'}`}
    </span>
  );
}
PipelineRuntimeArgsCounter.propTypes = {
  runtimeArgs: Proptypes.object,
};

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: getFilteredRuntimeArgs(ownProps.runtimeArgs || state.runtimeArgs),
  };
};
const ConnectedPipelinRuntimeArgsCounter = connect(mapStateToProps)(PipelineRuntimeArgsCounter);
export default ConnectedPipelinRuntimeArgsCounter;
