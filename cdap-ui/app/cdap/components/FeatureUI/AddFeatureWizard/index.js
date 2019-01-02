
import React from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import AddFeatureWizardConfig from '../../../services/WizardConfigs/AddFeatureWizardConfig';
import AddFeatureStore from '../../../services/WizardStores/AddFeature/AddFeatureStore';

class AddFeatureWizard extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      successInfo: {},
      activeStepId: 'schema',
    }
  }

  render() {
    return (
          <WizardModal
            title= "New Feature"
            isOpen={this.props.showWizard}
            toggle={this.props.onClose}
            className="add-feature-wizard">
            <Wizard
              wizardConfig={AddFeatureWizardConfig}
              wizardType="Add-Feature"
              onSubmit={this.props.onSubmit}
              successInfo={this.state.successInfo}
              onClose={this.props.onClose}
              activeStepId={this.state.activeStepId}
              store={AddFeatureStore}
            />
          </WizardModal>
    );
  }
}

export default AddFeatureWizard;