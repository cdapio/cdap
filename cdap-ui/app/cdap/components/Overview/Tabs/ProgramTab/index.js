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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';
import T from 'i18n-react';
import {MyProgramApi} from 'api/program';
import NamespaceStore from 'services/NamespaceStore';
import {convertProgramToApi} from 'services/program-api-converter';
import ViewSwitch from 'components/ViewSwitch';
import ProgramCards from 'components/ProgramCards';
import ProgramTable from 'components/ProgramTable';
import isEmpty from 'lodash/isEmpty';
require('./ProgramTab.scss');

export default class ProgramsTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      runningPrograms: []
    };
    this.statusSubscriptions = [];
    if (
      !isNil(this.props.entity) &&
      !isEmpty(this.props.entity)
    ) {
      this.setRunningPrograms();
    }
  }
  componentWillReceiveProps(nextProps) {
    let entitiesMatch = objectQuery(nextProps, 'entity', 'name') === objectQuery(this.props, 'entity', 'name');
    if (!entitiesMatch) {
      this.setState({
        entity: nextProps.entity,
        runningPrograms: []
      });
      this.statusSubscriptions.forEach(sub => sub.dispose());
      this.setRunningPrograms();
    }
  }
  componentWillUnmount() {
    this.statusSubscriptions.forEach(sub => sub.dispose());
  }
  setRunningPrograms() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    this.state
        .entity
        .programs
        .forEach(program => {
          let params = {
            namespace,
            appId: program.app,
            programType: convertProgramToApi(program.type),
            programId: program.id
          };
          let subscription =  MyProgramApi
            .pollRuns(params)
            .combineLatest(MyProgramApi.pollStatus(params))
            .subscribe(res => {
              let runningPrograms = this.state.runningPrograms;
              let programState = runningPrograms.filter(prog => prog.name === program.id && prog.app === program.app);
              if (programState.length) {
                runningPrograms = runningPrograms.filter(prog => prog.name !== program.id || (prog.name === program.id && prog.app !== program.app));
              }
              runningPrograms.push(Object.assign({}, !isEmpty(programState) ? programState[0] : {}, {
                latestRun: objectQuery(res, 0, 0) || {},
                status: res[1].status === 'RUNNING' ? 1 : 0,
                backendStatus: res[1].status,
                name: program.id,
                app: program.app
              }));
              this.setState({
                runningPrograms
              });
            });
          this.statusSubscriptions.push(subscription);
        });
  }
  render() {
    let runningProgramsCount = 0;
    let programsForTable = this.state.entity.programs;
    if (this.state.runningPrograms.length) {
      runningProgramsCount = this.state
        .runningPrograms
        .map(runningProgram => runningProgram.status)
        .reduce((prev, curr) => prev + curr);
      programsForTable = this.state.entity.programs.map(program => {
        let matchedProg = this.state.runningPrograms.find(prog => prog.name === program.id && prog.app === program.app) || null;
        return !matchedProg ?
          program
        :
          Object.assign({}, program, {
            status: matchedProg.backendStatus,
            latestRun: matchedProg.latestRun
          });
      });
    }
    if (!isNil(this.state.entity)) {
      if (this.state.entity.programs.length) {
        let title = T.translate('features.Overview.ProgramTab.title', {appId: this.state.entity.name});

        if (['dataset', 'stream'].indexOf(this.state.entity.type) !== -1) {
          title = T.translate('features.Overview.ProgramTab.altTitle', {
            entityId: this.state.entity.id,
            entityType: this.state.entity.type
          });
        }

        return (
          <div className="program-tab clearfix">
            <div className="message-section float-xs-left">
              <strong> {title} </strong>
              <div>{T.translate('features.Overview.ProgramTab.runningProgramLabel', {programCount: runningProgramsCount})}</div>
            </div>
            <ViewSwitch>
              <ProgramCards programs={this.state.entity.programs} />
              <ProgramTable programs={programsForTable} />
            </ViewSwitch>
          </div>
        );
      } else {
        return (
          <div className="program-tab clearfix">
            <i>{T.translate('features.Overview.ProgramTab.emptyMessage')}</i>
          </div>
        );
      }
    }
    return null;
  }
}
ProgramsTab.defaultProps = {
  entity: {
    programs: []
  }
};
ProgramsTab.propTypes = {
  entity: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    programs: PropTypes.arrayOf(PropTypes.shape({
      app: PropTypes.string,
      id: PropTypes.string,
      type: PropTypes.string
    })),
    type: PropTypes.string
  }))
};
