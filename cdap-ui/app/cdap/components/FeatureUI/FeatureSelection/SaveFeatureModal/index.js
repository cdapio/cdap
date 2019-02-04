import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import propTypes from 'prop-types';
import FEDataServiceApi from '../../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { isNil } from 'lodash';
import { checkResponseError,getErrorMessage } from '../../util';

require('./SaveFeatureModal.scss');


class SaveFeatureModal extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      title: 'Save',
      name: "",
      hasError:false,
      errorMessage:""
    };
    this.onOk = this.onOk.bind(this);
    this.onCancel = this.onCancel.bind(this);
  }

  nameChange = (evt) => {
    this.setState({ name: evt.target.value });
  }

  onCancel() {
    this.props.onClose('CANCEL');
    gridRowData: parsedResult.gridRowData
  }

  onOk() {
    this.setState({hasError:false, errorMessage:""});
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
          if (checkResponseError(result)) {
            const message = getErrorMessage(result, "couldn't save pipeline");
            this.setState({hasError:true, errorMessage:message});
          } else {
            this.setState({ name:""});
            this.props.onClose('OK');
          }
        },
        error => {
          this.setState({hasError:true, errorMessage:getErrorMessage(error, "couldn't save pipeline")});
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
  selectedPipeline: propTypes.object,
  selectedFeatures: propTypes.array
};
