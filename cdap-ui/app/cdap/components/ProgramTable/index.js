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

import React, {Component, PropTypes} from 'react';
import FastActions from 'components/EntityCard/FastActions';
import SortableTable from 'components/SortableTable';
import IconSVG from 'components/IconSVG';
import shortid from 'shortid';
import {humanReadableDate} from 'services/helpers';
import EntityIconMap from 'services/entity-icon-map';
import T from 'i18n-react';
import isEmpty from 'lodash/isEmpty';
import moment from 'moment';
require('./ProgramTable.scss');

export default class ProgramTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entities: []
    };

    this.tableHeaders = [
      {
        property: 'name',
        label: T.translate('features.ViewSwitch.nameLabel')
      },
      {
        property: 'programType',
        label: T.translate('features.ViewSwitch.typeLabel')
      },
      // have to convert latestRun back from string to seconds from epoch
      {
        property: 'latestRun',
        label: T.translate('features.ViewSwitch.ProgramTable.lastStartedLabel'),
        sortFunc: (entity) => { return moment(entity.latestRun.start).valueOf(); }
      },
      {
        property: 'status',
        label: T.translate('features.ViewSwitch.ProgramTable.statusLabel')
      },
      // empty header label for Actions column
      {
        label: ''
      }
    ];
  }

  componentWillMount() {
    let entities = this.updateEntities(this.props.programs);
    this.setState({
      entities
    });
  }

  componentWillReceiveProps(nextProps) {
    let entities = this.updateEntities(nextProps.programs);
    this.setState({
      entities
    });
  }

  updateEntities(programs) {
    return programs.map(prog => {
      return Object.assign({}, prog, {
        latestRun: prog.latestRun || {},
        applicationId: prog.app,
        programType: prog.type,
        type: 'program',
        id: prog.id,
        uniqueId: shortid.generate()
      });
    });
  }

  renderTableBody() {
    return (
      <tbody>
        {
          this.state.entities.map(program => {
            let icon = EntityIconMap[program.programType];
            let statusClass = program.status === 'RUNNING' ? 'text-success' : '';
            return (
              <tr key={program.uniqueId}>
                <td>
                  <span title={program.name}>
                    {program.name}
                  </span>
                </td>
                <td>
                  <IconSVG
                    name={icon}
                    className="program-type-icon"
                  />
                  {program.programType}
                </td>
                <td>
                  {
                    !isEmpty(program.latestRun) ? humanReadableDate(program.latestRun.start) : 'n/a'
                  }
                </td>
                <td className={statusClass}>
                  {
                    !isEmpty(program.status) ? program.status : 'n/a'
                  }
                </td>
                <td>
                  <div className="fast-actions-container text-xs-center">
                    <FastActions
                      className="text-xs-left btn-group"
                      entity={program}
                    />
                  </div>
                </td>
              </tr>
            );
          })
        }
      </tbody>
    );
  }

  render() {

    if (this.state.entities && Array.isArray(this.state.entities)) {
      if (this.state.entities.length) {
        return (
          <div className="program-table">
            <SortableTable
              entities={this.state.entities}
              tableHeaders={this.tableHeaders}
              renderTableBody={this.renderTableBody}
            />
          </div>
        );
      } else {
        return (
          <div className="history-tab">
            <i>
              {T.translate('features.Overview.ProgramTab.emptyMessage')}
            </i>
          </div>
        );
      }
    }
    return (
      <div className="program-table">
        <h3 className="text-xs-center">
          <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
        </h3>
      </div>
    );
  }
}
ProgramTable.propTypes = {
  programs: PropTypes.arrayOf(PropTypes.object)
};
