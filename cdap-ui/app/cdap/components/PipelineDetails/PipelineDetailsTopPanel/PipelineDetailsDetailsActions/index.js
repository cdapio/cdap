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
import PipelineDetailsDetailsButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsDetailsActions/PipelineDetailsDetailsButton';
import PipelineDetailsActionsButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsDetailsActions/PipelineDetailsActionsButton';
require('./PipelineDetailsDetailsActions.scss');

const mapDetailsStateToProps = (state) => {
  return {
    pipelineName: state.name,
    description: state.description,
    artifact: state.artifact,
    config: state.config
  };
};

const PipelineDetailsDetailsActions = ({pipelineName, description, artifact, config}) => {
  return (
    <div className="pipeline-details-buttons pipeline-details-details-actions">
      <PipelineDetailsDetailsButton pipelineName={pipelineName} />
      <PipelineDetailsActionsButton
        pipelineName={pipelineName}
        description={description}
        artifact={artifact}
        config={config}
      />
    </div>
  );
};

PipelineDetailsDetailsActions.propTypes = {
  pipelineName: PropTypes.string,
  description: PropTypes.string,
  artifact: PropTypes.object,
  config: PropTypes.object
};

const ConnectedPipelineDetailsDetailsActions = connect(mapDetailsStateToProps)(PipelineDetailsDetailsActions);
export default ConnectedPipelineDetailsDetailsActions;
