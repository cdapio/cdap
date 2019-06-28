/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import { any } from 'prop-types';

export enum Stages {
  CONFIGURE = 'configure',
  ASSESSMENT = 'assessment',
  PUBLISH = 'publish',
  VALIDATE = 'validate',
  PUBLISHED = 'published',
}

export const defaultContext = {
  id: null,
  stage: Stages.CONFIGURE,
  activeStep: 0,
  name: '',
  description: '',
  source: {},
  sourceConfig: {},
  target: {},
  targetConfig: {},
  // tslint:disable:no-empty
  next: () => {},
  previous: () => {},
  setNameDescription: (id, name, description) => {},
  setSource: (source, sourceConfig) => {},
  setTarget: (target, targetConfig) => {},
  setActiveStep: (step) => {},
  setStage: (stage) => {},
  getRequestBody: (activeStep): any => {},
  // tslint:enable:no-empty
};

export const TransfersCreateContext = React.createContext(defaultContext);

export const transfersCreateConnect = (Comp) => {
  return (extraProps) => {
    return (
      <TransfersCreateContext.Consumer>
        {(props) => {
          const finalProps = {
            ...props,
            ...extraProps,
          };

          return <Comp {...finalProps} />;
        }}
      </TransfersCreateContext.Consumer>
    );
  };
};
