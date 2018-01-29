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
import React from 'react';
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import {getHyperParamLabel} from 'components/Experiments/store/ActionCreator';

require('./HyperParamsPopover.scss');

export default function HyperParamsPopover({algorithm, hyperparameters}) {
  return (
    <Popover
      target={() => <IconSVG name="icon-cogs" />}
      className="hyperparameters-popover"
      placement="right"
      bubbleEvent={false}
      enableInteractionInPopover={true}
    >
      <table className="table table-bordered">
        <thead>
          <tr>
            <th>Hyperparameter</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {
            Object
              .keys(hyperparameters)
              .map((param, i) => {
                return (
                  <tr key={i}>
                    <td>{getHyperParamLabel(algorithm, param)}</td>
                    <td>{hyperparameters[param]} </td>
                  </tr>
                );
              })
          }
        </tbody>
      </table>
    </Popover>
  );
}
HyperParamsPopover.propTypes = {
  hyperparameters: PropTypes.object,
  algorithm: PropTypes.string
};
