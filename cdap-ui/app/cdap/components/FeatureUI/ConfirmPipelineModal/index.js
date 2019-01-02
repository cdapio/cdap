
import React from 'react';
import { Button, Input, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
require('./ConfirmPipelineModal.scss');

class ConfirmPipelineModal extends React.Component {
  name;
  constructor(props) {
    super(props);
  }

  onDone() {
    this.props.onSave(this.name);
  }

  onNameUpdated() {
    this.name =  event.target.value;
  }

  render() {
    return (
      <div>
        <Modal isOpen={this.props.open}
          zIndex='1070'>
          <ModalHeader >Feature Pipelne</ModalHeader>
          <ModalBody>
            <div className='field-row'>
                <div className='name'>Name</div>
                <div className='colon'>:</div>
                <Input className='value' type="text" name="value" placeholder='value'
                  defaultValue = {this.props.name} onChange={this.onNameUpdated.bind(this)}/>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button className = "btn-margin" color="secondary" onClick={this.props.onClose}>Cancel</Button>
            <Button className = "btn-margin" color="primary" onClick={this.onDone.bind(this)}>Confirm</Button>{' '}
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default ConfirmPipelineModal;