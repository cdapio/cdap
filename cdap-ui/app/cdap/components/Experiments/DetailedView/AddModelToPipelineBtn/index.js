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
import React, {Component} from 'react';
import {MyArtifactApi} from 'api/artifact';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {isNilOrEmpty, objectQuery} from 'services/helpers';
import getPipelineConfig from 'components/Experiments/DetailedView/AddModelToPipelineBtn/PipelineSkeleton';
import {GLOBALS} from 'services/global-constants';
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';
import { connect } from 'react-redux';
import uuidV4 from 'uuid/v4';
require('./AddModelToPipelineBtn.scss');

const MMDS_PLUGINS_ARTIFACT_NAME = 'mmds-plugins';
const ERROR_MSG = 'Unable to find plugins to create a scoring pipeline';

class AddModelToPipelineBtn extends Component {
  static propTypes = {
    experimentId: PropTypes.string,
    modelId: PropTypes.string,
    modelName: PropTypes.string
  };

  state = {
    disabled: true,
    error: null,
    mmdsPluginsArtifact: null,
    datapipelineArtifact: null,
    wranglerArtifact: null
  };

  cloneId = uuidV4();
  batchPipelineUrl = window.getHydratorUrl({
    stateName: 'hydrator.create',
    stateParams: {
      namespace: getCurrentNamespace(),
      cloneId: this.cloneId,
      artifactType: GLOBALS.etlDataPipeline
    }
  });

  componentDidMount() {
    this.fetchArtifactForPipelines();
  }
  fetchArtifactForPipelines() {
    MyArtifactApi
      .list({
        namespace: getCurrentNamespace()
      })
      .subscribe(
        (res) => {
          let mmdsPluginsArtifact, datapipelineArtifact, wranglerArtifact;
          res.forEach(artifact => {
            if (artifact.name === MMDS_PLUGINS_ARTIFACT_NAME) {
              mmdsPluginsArtifact = artifact;
            }
            if (artifact.name === GLOBALS.etlDataPipeline) {
              datapipelineArtifact = artifact;
            }
            if (artifact.name === GLOBALS.wrangler.artifactName) {
              wranglerArtifact = artifact;
            }
          });
          if (
            isNilOrEmpty(mmdsPluginsArtifact) ||
            isNilOrEmpty(datapipelineArtifact) ||
            isNilOrEmpty(wranglerArtifact)
          ) {
            this.setState({
              error: ERROR_MSG
            });
          } else {
            this.setState({
              mmdsPluginsArtifact,
              datapipelineArtifact,
              wranglerArtifact,
              disabled: false
            });
          }
        },
        () => {
          this.setState({
            error: ERROR_MSG
          });
        }
      );
  }
  generatePipelineConfig = () => {
    let {experimentId, modelId, modelName} = this.props;
    let {mmdsPluginsArtifact, datapipelineArtifact} = this.state;
    let pipelineConfig = getPipelineConfig({
      mmdsPluginsArtifact,
      experimentId,
      modelId
    });
    pipelineConfig = {
      ...pipelineConfig,
      name: `Scoring_Pipeline_${experimentId}_${modelName}`,
      description: `Scoring pipeline for ${modelName} under experiment ${experimentId}.`,
      artifact: datapipelineArtifact
    };
    window.localStorage.setItem(this.cloneId, JSON.stringify(pipelineConfig));
  }

  render() {
    return (
      <fielset className="add-model-to-pipeline" disabled={this.state.disabled}>
        <a
          className="btn btn-primary"
          onClick={this.generatePipelineConfig}
          href={this.state.disabled ? null : this.batchPipelineUrl}
        >
          <span>Create a scoring pipeline</span>
          {
            this.state.error ?
              <Popover
                target={() => <IconSVG name="icon-exclamation-triangle" />}
                showOn="Hover"
              >
                {this.state.error}
              </Popover>
            :
              null
          }
        </a>
      </fielset>
    );
  }
}

const mapStateToProps = (state, ownProps) => {
  let modelObj = state.models.find(model => model.id === ownProps.modelId);
  return {
    experimentId: state.name,
    modelId: ownProps.modelId,
    directives: objectQuery(modelObj, 'directives'),
    modelName: objectQuery(modelObj, 'name'),
    srcPath: state.srcpath
  };
};

const ConnectedAddModelToPipelineBtn = connect(mapStateToProps)(AddModelToPipelineBtn);
export default ConnectedAddModelToPipelineBtn;
