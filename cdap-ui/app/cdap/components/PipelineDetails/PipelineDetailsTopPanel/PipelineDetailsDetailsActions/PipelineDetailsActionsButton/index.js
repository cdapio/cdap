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
import React from 'react';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import {getCurrentNamespace} from 'services/NamespaceStore';
require('./PipelineDetailsActionsButton.scss');

export default function PipelineDetailsActionsButton({pipelineName, description, artifact, config}) {
  const getClonePipelineName = (name) => {
    name = typeof a === 'string' ? name : name.toString();
    let version = name.match(/(_v[\d]*)$/g);
    let existingSuffix; // For cases where pipeline name is of type 'SamplePipeline_v2_v4_v333'
    if (Array.isArray(version)) {
      version = version.pop();
      existingSuffix = version;
      version = version.replace('_v', '');
      version = '_v' + ((!isNaN(parseInt(version, 10)) ? parseInt(version, 10) : 1) + 1);
    } else {
      version = '_v1';
    }
    return name.split(existingSuffix)[0] + version;
  };

  const duplicateConfigAndNavigate = () => {
    let bumpedVersionName = getClonePipelineName(pipelineName);
    const pipelineConfig = {
      name: bumpedVersionName,
      description,
      artifact,
      config
    };
    window.localStorage.setItem(bumpedVersionName, JSON.stringify(pipelineConfig));
    const hydratorLink = window.getHydratorUrl({
      stateName: 'hydrator.create',
      stateParams: {
        namespace: getCurrentNamespace(),
        cloneId: bumpedVersionName,
        artifactType: artifact.name
      }
    });
    window.location.href = hydratorLink;
  };

  const ActionsBtnAndLabel = () => {
    return (
      <div className="btn pipeline-action-btn pipeline-actions-btn">
        <div className="btn-container">
          <IconSVG name="icon-cog" />
          <div className="button-label">
            Actions
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="pipeline-action-container pipeline-actions-container">
      <Popover
        target={ActionsBtnAndLabel}
        placement="bottom"
        bubbleEvent={false}
        className="pipeline-actions-popper"
      >
        <div>
          <a onClick={duplicateConfigAndNavigate}>
            Duplicate
          </a>
        </div>
        <div>Export</div>
        <div>Delete Pipeline</div>
      </Popover>
    </div>
  );
}

PipelineDetailsActionsButton.propTypes = {
  pipelineName: PropTypes.string,
  description: PropTypes.string,
  artifact: PropTypes.object,
  config: PropTypes.object
};
