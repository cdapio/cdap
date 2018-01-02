/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import React from 'react';
import PipelineTriggers from 'components/PipelineTriggers';
import TriggeredPipelines from 'components/TriggeredPipelines';

export default function PipelineTriggersSidebars({pipelineName, namespace}) {
  return (
    <div className="pipeline-triggers-sidebar-container">
      <PipelineTriggers
        pipelineName={pipelineName}
        namespace={namespace}
      />
      <TriggeredPipelines
        pipelineName={pipelineName}
      />
    </div>
  );
}

PipelineTriggersSidebars.propTypes = {
  pipelineName: PropTypes.string,
  namespace: PropTypes.string
};
