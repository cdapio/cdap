import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import propTypes from 'prop-types';
import FEDataServiceApi from '../../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { isNil } from 'lodash';
import { checkResponseError } from '../../util';
require('./SaveFeatureModal.scss');
import {
  GET_FEATURE_CORRELAION
} from '../../config';

class SaveFeatureModal extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      title: 'Save',
      name:""
    };
    this.onOk = this.onOk.bind(this);
    this.onCancel = this.onCancel.bind(this);
  }

  nameChange = (evt)=>{
    this.setState({ name: evt.target.value });
  }

  onCancel() {
    this.props.onClose('CANCEL');
  }

  onOk() {
    this.savePipeline();
  }


  savePipeline = () => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    FEDataServiceApi.saveFeaturePipeline(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,

      }, this.getSavePipelineRequest(featureGenerationPipelineName)).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureCorrelationScores"])) {
            this.handleError(result, GET_FEATURE_CORRELAION);
          } else {
            this.props.onClose('OK');
            const parsedResult = this.praseCorrelation(result["featureCorrelationScores"]);
            this.setState({ gridColumnDefs: parsedResult.gridColumnDefs });
            this.setState({ gridRowData: parsedResult.gridRowData });
          }
        },
        error => {
          this.handleError(error, GET_FEATURE_CORRELAION);
        }
      );

  }

  getSavePipelineRequest(value) {
    return {
      selectedFeatures: this.props.selectedFeatures.map((item)=>item.featureName),
      featureEngineeringPipeline:value,
      featureSelectionPipeline:this.state.name,
    };
  }

  render() {
    return (
      <div className="save-pipeline-box">
        <Modal isOpen={this.props.open} zIndex="1090">
          <ModalHeader>{this.state.title}</ModalHeader>
          <ModalBody>
            <div>
              <label className="pipeline-label">Pipeline Name :</label>
              <input className="pipeline-name" value={this.state.name}
               onChange={this.nameChange}></input>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button className="btn-margin" color="secondary" onClick={this.onCancel}>Cancel</Button>
            <Button className="btn-margin" color="primary" onClick={this.onOk}
              disabled={this.state.name.trim().length < 1} >OK</Button>
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default SaveFeatureModal;
SaveFeatureModal.propTypes = {
  onClose: propTypes.func,
  open: propTypes.bool,
  selectedPipeline:propTypes.object,
  selectedFeatures:propTypes.array
};
