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

import React, { PropTypes } from 'react';
import LoadingSVG from 'components/LoadingSVG';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';

const PREFIX = `features.TriggeredPipelines`;

export default function TriggeredPipelineRow({isExpanded, pipeline, onToggle, loading, pipelineInfo, sourcePipeline}) {
  if (!isExpanded) {
    return (
      <div
        className="triggered-pipeline-row"
        onClick={onToggle.bind(null, pipeline)}
      >
        <div className="caret-container">
          <IconSVG name="icon-caret-right" />
        </div>
        <div className="pipeline-name">
          {pipeline.application}
        </div>
        <div className="namespace">
          {pipeline.namespace}
        </div>
      </div>
    );
  }

  const loadingIcon = (
    <div className="text-xs-center text-center">
      <LoadingSVG />
    </div>
  );

  return (
    <div className="triggered-pipeline-expanded-row">
      <div
        className="header-row"
        onClick={onToggle.bind(null, null)}
      >
        <div className="caret-container">
          <IconSVG name="icon-caret-down" />
        </div>

        <div className="pipeline-name">
          {pipeline.application}
        </div>
        <div className="namespace">
          {pipeline.namespace}
        </div>
      </div>

      {
        loading ?
          loadingIcon
        :
          (
            <div className="row-content">
              <div className="pipeline-description">
                <strong>{T.translate(`${PREFIX}.description`)}: </strong>
                <span>
                  {pipelineInfo.description}
                </span>
                <a href={`/pipelines/ns/${pipeline.namespace}/view/${pipeline.application}`}>
                  {T.translate(`${PREFIX}.viewPipeline`)}
                </a>
              </div>

              <div className="helper-text">
                {T.translate(`${PREFIX}.helperText`, {pipelineName: sourcePipeline})}
              </div>

              <div className="events-list">
                {
                  pipeline.trigger.programStatuses.map((status) => {
                    return <div>- {T.translate(`${PREFIX}.Events.${status}`)}</div>;
                  })
                }
              </div>
            </div>
          )
      }
    </div>
  );
}

TriggeredPipelineRow.propTypes = {
  isExpanded: PropTypes.bool,
  pipeline: PropTypes.object,
  onToggle: PropTypes.func,
  loading: PropTypes.bool,
  pipelineInfo: PropTypes.object,
  sourcePipeline: PropTypes.string
};

