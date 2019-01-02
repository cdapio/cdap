
import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
require('./AlertModal.scss');

class AlertModal extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      title: 'Alert'
    }
    this.onOk = this.onOk.bind(this);
    this.onCancel = this.onCancel.bind(this);
  }

  onCancel(){
    this.props.onClose('CANCEL')
  }

  onOk(){
    this.props.onClose('OK')
  }

  render() {
    return (
      <div>
        <Modal isOpen={this.props.open}  zIndex="1090"
        onRequestClose = {this.onCancel}>
          <ModalHeader>{this.state.title}</ModalHeader>
          <ModalBody>
            <div>{this.props.message}</div>
          </ModalBody>
          <ModalFooter>
            <Button className = "btn-margin" color="secondary" onClick={this.onCancel}>Cancel</Button>
            <Button className = "btn-margin" color="primary" onClick={this.onOk}>OK</Button>{' '}
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default AlertModal;