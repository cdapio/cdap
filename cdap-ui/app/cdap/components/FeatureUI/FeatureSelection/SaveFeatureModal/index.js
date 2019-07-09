
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

import React from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import PropTypes from 'prop-types';
import FEDataServiceApi from '../../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { isNil } from 'lodash';
import T from 'i18n-react';
import { checkResponseError,getErrorMessage, getDefaultRequestHeader } from '../../util';
import { ERROR_MESSAGES, SAVE_PIPELINE } from 'components/FeatureUI/config';

require('./SaveFeatureModal.scss');
const PREFIX = 'features.FeatureEngineering.FeatureSelection.SaveFeatureModal';

class SaveFeatureModal extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      loading: false,
      title: T.translate(`${PREFIX}.title`),
      name: "",
      hasError: false,
      errorMessage: ""
    };
    this.onOk = this.onOk.bind(this);
    this.onCancel = this.onCancel.bind(this);
  }

  nameChange = (evt) => {
    this.setState({ name: evt.target.value });
  }

  onCancel() {
    this.setState({name:"",hasError:false, errorMessage:"",loading:false});
    this.props.onClose(T.translate(`${PREFIX}.cancelButton`));
  }

  onOk() {
    this.setState({hasError:false, errorMessage:"",loading:true});
    this.savePipeline();
  }

  savePipeline = () => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    FEDataServiceApi.saveFeaturePipeline(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,

      }, this.getSavePipelineRequest(featureGenerationPipelineName), getDefaultRequestHeader()).subscribe(
        result => {
          if (checkResponseError(result)) {
            const message = getErrorMessage(result, ERROR_MESSAGES[SAVE_PIPELINE]);
            this.setState({hasError:true, errorMessage:message,loading:false});
          } else {
            this.setState({ name:"",loading:false});
            this.props.onClose(T.translate(`${PREFIX}.okButton`));
          }
        },
        error => {
          this.setState({hasError:true, errorMessage:getErrorMessage(error, ERROR_MESSAGES[SAVE_PIPELINE]),loading:false});
        }
      );

  }

  getSavePipelineRequest(value) {
    return {
      selectedFeatures: this.props.selectedFeatures.map((item) => item.featureName),
      featureEngineeringPipeline: value,
      featureSelectionPipeline: this.state.name,
    };
  }

  render() {
    return (
      <div className="save-pipeline-box">
        <Modal isOpen={this.props.open} zIndex="1090" className="modal-box">
          <ModalHeader>{this.state.title}</ModalHeader>
          <ModalBody>
            <div>
              <label className="pipeline-label">Pipeline Name :</label>
              <input className="pipeline-name" value={this.state.name}
                onChange={this.nameChange}></input>
            </div>
          </ModalBody>
          <ModalFooter>
            {
              this.state.hasError ?
                <label className="error-box">{this.state.errorMessage}</label>
                : null
            }
            <fieldset disabled={this.state.loading} className='button-container'>
              <button
                className="btn btn-primary ok-btn"
                onClick={this.onOk}
                disabled={this.state.name.trim().length < 1}>
                {
                  this.state.loading ?
                    <span className="fa fa-spin fa-spinner" />
                  :
                    null
                }
                <span className="apply-label">{T.translate(`${PREFIX}.okButton`)}</span>
              </button>
              <button
                className="btn btn-secondary"
                onClick={this.onCancel}
              >{T.translate(`${PREFIX}.cancelButton`)}</button>
            </fieldset>
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default SaveFeatureModal;
SaveFeatureModal.propTypes = {
  onClose: PropTypes.func,
  open: PropTypes.any,
  selectedPipeline: PropTypes.object,
  selectedFeatures: PropTypes.array
};
