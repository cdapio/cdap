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

import React, {PropTypes} from 'react';
import FastActions from 'components/EntityCard/FastActions';
import shortid from 'shortid';
import {humanReadableDate} from 'services/helpers';
require('./ProgramTable.scss');
import T from 'i18n-react';
import isEmpty from 'lodash/isEmpty';

export default function ProgramTable({programs}) {
  let entities = programs.map(prog => {
    return Object.assign({}, prog, {
      applicationId: prog.app,
      programType: prog.type,
      type: 'program',
      id: prog.id,
      uniqueId: shortid.generate()
    });
  });
  return (
    <div className="program-table">
      <table className="table table-bordered">
      <thead>
        <tr>
          <th>{T.translate('features.ViewSwitch.nameLabel')}</th>
          <th>{T.translate('features.ViewSwitch.typeLabel')}</th>
          <th>{T.translate('features.ViewSwitch.ProgramTable.lastStartedLabel')}</th>
          <th>{T.translate('features.ViewSwitch.ProgramTable.statusLabel')}</th>
          <th>{T.translate('features.ViewSwitch.actionsLabel')}</th>
        </tr>
      </thead>
      <tbody>
        {
          entities.map(program => {
            return (
              <tr key={program.name}>
                <td>{program.name}</td>
                <td>{program.programType}</td>
                <td>{
                  !isEmpty(program.latestRun) ? humanReadableDate(program.latestRun.start) : 'n/a'
                }</td>
                <td>{program.status}</td>
                <td>
                  <div className="fast-actions-container">
                    <FastActions
                      className="text-xs-left"
                      entity={program}
                    />
                  </div>
                </td>
              </tr>
            );
          })
        }
      </tbody>
      </table>
    </div>
  );
}
ProgramTable.propTypes = {
  programs: PropTypes.arrayOf(PropTypes.object)
};
