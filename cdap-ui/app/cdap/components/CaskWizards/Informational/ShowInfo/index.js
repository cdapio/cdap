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

import PropTypes from 'prop-types';

import React from 'react';
import { connect, Provider } from 'react-redux';
import InformationalWizardStore from 'services/WizardStores/Informational/InformationalStore';
require('./ShowInfo.scss');

const mapStateWithProps = (state) => {
  return {
    steps: state.information.steps
  };
};

let StepsList = ({steps}) => {
  function createMarkup(content) {
    return { __html: content };
  }

  const regex = /(http|https|ftp|ftps)\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3}(\/\S*)?/g;
  const urlifiedSteps = steps.map((step) => {
    return step.replace(regex, (url) => {
      let anchorLink = document.createElement('a');
      anchorLink.href = url;
      return `<a href="${url}" title="${url}" target="_blank"> ${anchorLink.hostname.replace(/^www./g, '')} </a>`;
    });
  });

  return (
    <ul className="list-unstyled info-list">
      {
        urlifiedSteps.map((step, index) => {
          return (
            <li key={index}>
              <span className="step-number">
                {index + 1}
              </span>
              <span
                className="step-text"
                dangerouslySetInnerHTML={createMarkup(step)}
              />
            </li>
          );
        })
      }
    </ul>
  );
};

StepsList.propTypes = {
  steps: PropTypes.array,
};

StepsList = connect(
  mapStateWithProps,
  null
)(StepsList);

export default function ShowInfo() {
  return (
    <Provider store={InformationalWizardStore}>
      <StepsList />
    </Provider>
  );
}
