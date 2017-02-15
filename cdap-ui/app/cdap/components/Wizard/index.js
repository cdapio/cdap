/*
 * Copyright © 2016 Cask Data, Inc.
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
import React, {Component, PropTypes} from 'react';
import Rx from 'rx';
import findIndex from 'lodash/findIndex';
import first from 'lodash/head';
import T from 'i18n-react';

require('./Wizard.scss');
import shortid from 'shortid';
import isEmpty from 'lodash/isEmpty';
import WizardStepHeader from './WizardStepHeader';
import WizardStepContent from './WizardStepContent';
import CardActionFeedback from 'components/CardActionFeedback';
import classnames from 'classnames';

const currentStepIndex = (arr, id) => {
  return findIndex(arr, (step) => id === step.id);
};
const canFinish = (id, wizardConfig) => {
  let stepIndex = currentStepIndex(wizardConfig.steps, id);
  let canFinish = true;
  for (var i = stepIndex + 1; i < wizardConfig.steps.length; i++) {
    let reqsFields = Array.isArray(wizardConfig.steps[i].requiredFields);
    canFinish = canFinish && (!reqsFields || (reqsFields && reqsFields.length === 0));
  }
  return canFinish;
};

export default class Wizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeStep: this.props.wizardConfig.steps[0].id,
      loading: false,
      error: '',
      requiredStepsCompleted: false
    };
  }
  componentWillMount() {
    this.checkRequiredSteps();
    this.storeSubscription = this.props.store.subscribe(() => {
      this.checkRequiredSteps();
    });
  }
  componentWillUnmount() {
    this.storeSubscription();
  }
  checkRequiredSteps() {
    let state = this.props.store.getState();
    let steps = Object.keys(state);

    // non-required steps are marked as complete by default
    let requiredStepsCompleted = steps.every(key => {
      return state[key].__complete;
    });
    if (this.state.requiredStepsCompleted != requiredStepsCompleted) {
      this.setState({
        requiredStepsCompleted
      });
    }
  }
  getChildContext() {
    return {
      activeStep: this.state.activeStep
    };
  }
  setActiveStep(stepId) {
    this.setState({
      activeStep: stepId
    });
  }
  goToNextStep(stepId) {
    let currentStep = findIndex(
      this.props.wizardConfig.steps,
      (step) => stepId === step.id
    );
    this.setActiveStep(this.props.wizardConfig.steps[currentStep + 1].id);
  }
  goToPreviousStep(stepId) {
    let currentStep = findIndex(
      this.props.wizardConfig.steps,
      (step) => stepId === step.id
    );
    this.setActiveStep(this.props.wizardConfig.steps[currentStep - 1].id);
  }
  submitForm() {
    let onSubmitReturn = this.props.onSubmit(this.props.store);
    this.setState({loading: true});
    if (onSubmitReturn instanceof Rx.Observable) {
      onSubmitReturn
      .subscribe(
        () => {
          this.setState({
            error: false,
            loading: false
          });
          if (this.props.successInfo && !isEmpty(this.props.successInfo)) {
            this.setState({callToActionInfo: this.props.successInfo});
          } else {
            this.props.onClose(true);
          }
        },
        (err) => {
          if (err) {
            this.setState({
              error: err,
              loading: false
            });
          }
        }
      );
    }
  }
  isStepComplete(stepId) {
    let state = this.props.store.getState()[stepId];
    return state && state.__complete;
  }

  getNavigationButtons(matchedStep) {
    let matchedIndex = currentStepIndex(this.props.wizardConfig.steps, matchedStep.id);
    let navButtons;
    let nextButton = (
      <button
        className="btn btn-secondary"
        onClick={this.goToNextStep.bind(this, matchedStep.id)}
      >
        <span>Next</span>
        <span className="fa fa-chevron-right"></span>
      </button>
    );
    let prevButton = (
      <button
        className="btn btn-secondary"
        onClick={this.goToPreviousStep.bind(this, matchedStep.id)}
      >
        <span className="fa fa-chevron-left"></span>
        <span>Previous</span>
      </button>
    );
    let finishButton = (
      <button
        className="btn btn-primary"
        onClick={this.submitForm.bind(this)}
        disabled={(!this.state.requiredStepsCompleted || this.state.loading) ? 'disabled' : null}
      >
        Finish
      </button>
    );
    // This is ugly. We need to find a better way.
    if (matchedIndex === 0 && this.props.wizardConfig.steps.length > 1) {
      navButtons = (
        <span>
          {canFinish(matchedStep.id, this.props.wizardConfig) ? finishButton : null}
          {nextButton}
        </span>
      );
    }
    if (matchedIndex === this.props.wizardConfig.steps.length - 1 && this.props.wizardConfig.steps.length > 1) {
      navButtons = (
        <span>
          {prevButton}
          {canFinish(matchedStep.id, this.props.wizardConfig) ? finishButton : null}
        </span>
      );
    }
    if (matchedIndex !== 0 &&
        matchedIndex !== this.props.wizardConfig.steps.length - 1 &&
        this.props.wizardConfig.steps.length > 1) {
      navButtons = (
        <span>
          {prevButton}
          {canFinish(matchedStep.id, this.props.wizardConfig) ? finishButton : null}
          {nextButton}
        </span>
      );
    }
    if (this.props.wizardConfig.steps.length === 1) {
      navButtons = (
        <span>
          {finishButton}
        </span>
      );
    }

    return navButtons;
  }

  getStepHeaders() {
    let stepHeaders = this.props
      .wizardConfig
      .steps
      .map( (step) => (
        <WizardStepHeader
          label={`${step.shorttitle}`}
          className={ this.isStepComplete(step.id) ? 'completed' : null}
          id={step.id}
          key={shortid.generate()}
          onClick={this.setActiveStep.bind(this, step.id)}
        />
      ));

    return stepHeaders;
  }

  getStepContent() {
    let stepContent = this.props
      .wizardConfig
      .steps
      .filter(step => step.id === this.state.activeStep)
      .map((matchedStep) => {
        return (
          <WizardStepContent
            title={matchedStep.title}
            description={matchedStep.description}
            stepsCount={this.props.wizardConfig.steps.length}
            currentStep={currentStepIndex(this.props.wizardConfig.steps, matchedStep.id) + 1}
          >
            {matchedStep.content}
            <div className="text-xs-right wizard-navigation">
              {this.getNavigationButtons(matchedStep)}
            </div>
          </WizardStepContent>
        );
      });

    stepContent = first(stepContent);
    return stepContent;
  }

  getCallsToAction() {
    let callToActionInfo = this.state.callToActionInfo;
    return (
      <div>
        <div
          className="close-section float-xs-right"
          onClick={this.props.onClose.bind(null, true)}
        >
          <span className="fa fa-times" />
        </div>
        <div className="result-container">
          <span className="success-message">
            {callToActionInfo.message}
            <p>{callToActionInfo.subtitle}</p>
          </span>
          <div className="clearfix">
            <a
              href={callToActionInfo.buttonUrl}
              className="call-to-action btn btn-primary"
            >
              {callToActionInfo.buttonLabel}
            </a>
            <a
              href={callToActionInfo.linkUrl}
              className="secondary-call-to-action text-white"
            >
              {callToActionInfo.linkLabel}
            </a>
          </div>
        </div>
      </div>
    );
  }

  getWizardFooter() {
    let wizardFooter = null;
    if (this.state.callToActionInfo) {
      wizardFooter = this.getCallsToAction();
    }
    if (this.state.error) {
      let step = T.translate(`features.Wizard.${this.props.wizardType}.headerlabel`);
      wizardFooter = (
        <CardActionFeedback
          type='DANGER'
          message={T.translate('features.Wizard.FailedMessage', {step})}
          extendedMessage={this.state.error}
        />
      );
    }
    if (this.state.loading) {
      wizardFooter = (
        <CardActionFeedback
          type='LOADING'
          message="Loading"
        />
      );
    }
    return wizardFooter;
  }

  render() {
    return (
      <div className="cask-wizard">
        <div className="wizard-body">
          <div className="wizard-steps-header">
            {this.getStepHeaders()}
          </div>
          <div className="wizard-steps-content">
            {this.getStepContent()}
          </div>
        </div>
        <div className={classnames("wizard-footer", {success: this.state.callToActionInfo})}>
          {this.getWizardFooter()}
        </div>
      </div>
    );
  }
}

const wizardConfigType = PropTypes.shape({
  steps: PropTypes.arrayOf(
    PropTypes.shape({
      name: PropTypes.string,
      title: PropTypes.string,
      description: PropTypes.string,
      content: PropTypes.node,
      skipToComplete: PropTypes.bool
    })
  )
});

Wizard.childContextTypes = {
  activeStep: PropTypes.string
};
Wizard.propTypes = {
  wizardConfig: wizardConfigType.isRequired,
  wizardType: PropTypes.string,
  store: PropTypes.shape({
    getState: PropTypes.func,
    dispatch: PropTypes.func,
    subscribe: PropTypes.func,
    replaceReducer: PropTypes.func
  }).isRequired,
  onSubmit: PropTypes.func.isRequired,
  successInfo: PropTypes.object,
  onClose: PropTypes.func.isRequired,
};
