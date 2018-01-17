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
import {connect, Provider} from 'react-redux';
import PipelineDetailStore from 'components/PipelineDetails/store';
import Tags from 'components/Tags';
import IconSVG from 'components/IconSVG';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import {GLOBALS} from 'services/global-constants';

const mapStateToPipelineTagsProps = (state) => {
  let {name} = state;
  return {
    entity: {
      id: name,
      type: 'application'
    },
    showCountLabel: false,
    isNativeLink: true
  };
};

const ConnectedPipelineTags = connect(mapStateToPipelineTagsProps)(Tags);

const mapStateToPipelineDetailsMetadataProps = (state) => {
  let {name, artifact, version, description} = state;
  return {
    name,
    artifactName: artifact.name,
    version,
    description
  };
};

const PipelineDetailsMetadata = ({name, artifactName, version, description}) => {
  return (
    <div className="pipeline-metadata">
      <div className="pipeline-type-name-version">
        <span className="pipeline-type">
          {
            artifactName === GLOBALS.etlDataPipeline ?
              <IconSVG name = "icon-ETLBatch" />
            :
              <IconSVG name = "icon-sparkstreaming" />
          }
        </span>
        <h1 className="pipeline-name">
          {name}
        </h1>
        <span className="pipeline-description">
          <IconSVG
            id="pipeline-description-info-icon"
            name="icon-info-circle"
          />
          <UncontrolledTooltip
            target="pipeline-description-info-icon"
            delay={{show: 250, hide: 0}}
          >
            {description}
          </UncontrolledTooltip>
        </span>
        <span className="pipeline-version">
          version {version}
        </span>
      </div>
      <div className="pipeline-tags">
        <ConnectedPipelineTags />
      </div>
    </div>
  );
};

PipelineDetailsMetadata.propTypes = {
  name: PropTypes.string,
  artifactName: PropTypes.string,
  version: PropTypes.string,
  description: PropTypes.string
};

const ConnectedPipelineDetailsMetadata = connect(mapStateToPipelineDetailsMetadataProps)(PipelineDetailsMetadata);

const ProvidedPipelineDetailsMetadata = () =>  {
  return (
    <Provider store={PipelineDetailStore}>
      <ConnectedPipelineDetailsMetadata />
    </Provider>
  );
};

export default ProvidedPipelineDetailsMetadata;
