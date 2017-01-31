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

import React, {PropTypes, Component} from 'react';
import EntityCard from 'components/EntityCard';
import {parseMetadata} from 'services/metadata-parser';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';
import T from 'i18n-react';
import {MyProgramApi} from 'api/program';
import NamespaceStore from 'services/NamespaceStore';
import {convertProgramToApi} from 'services/program-api-converter';
import shortid from 'shortid';

require('./ProgramTab.scss');

export default class ProgramsTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity,
      runningPrograms: []
    };
    this.statusSubscriptions = [];
    this.setRunninPrograms();
  }
  componentWillReceiveProps(nextProps) {
    let entitiesMatch = objectQuery(nextProps, 'entity', 'name') === objectQuery(this.props, 'entity', 'name');
    if (!entitiesMatch) {
      this.setState({
        entity: nextProps.entity,
        runningPrograms: []
      });
      this.statusSubscriptions.forEach(sub => sub());
      this.setRunninPrograms();
    }
  }
  setRunninPrograms() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    this.state
        .entity
        .programs
        .forEach(program => {
          let sub = MyProgramApi
            .pollStatus({
              namespace,
              appId: this.state.entity.name,
              programType: convertProgramToApi(program.type),
              programId: program.id
            })
            .subscribe(res => {
              let runningPrograms = this.state.runningPrograms;
              let programState = runningPrograms.filter(prog => prog.name === program.id);
              if (programState.length) {
                runningPrograms = runningPrograms.filter(prog => prog.name !== program.id);
              }
              runningPrograms.push({
                name: program.id,
                status: res.status === 'RUNNING' ? 1 : 0
              });
              this.setState({
                runningPrograms
              });
            });
          this.statusSubscriptions.push(sub);
        });
  }
  componentWillUnmount() {
    this.statusSubscriptions.forEach(sub => sub.dispose());
  }
  render() {
    let runningProgramsCount = 0;
    if (this.state.runningPrograms.length) {
      runningProgramsCount = this.state
        .runningPrograms
        .map(runningProgram => runningProgram.status)
        .reduce((prev, curr) => prev + curr);
    }
    if (!isNil(this.state.entity)) {
      return (
        <div className="program-tab">
          <div className="message-section">
            <strong> {T.translate('features.Overview.ProgramTab.title', {appId: this.state.entity.name})} </strong>
            <div>{T.translate('features.Overview.ProgramTab.runningProgramLabel', {programCount: runningProgramsCount})}</div>
          </div>
          {
            this.state.entity.programs.length ?
              this.state
                  .entity
                  .programs
                  .map( program => {
                    let entity = {
                      entityId: {
                        id: {
                          id: program.id,
                          application: {
                            applicationId: program.app
                          },
                          type: program.type
                        },
                        type: 'program',
                      },
                      metadata: {
                        SYSTEM: {}
                      }
                    };
                    entity = parseMetadata(entity);
                    let uniqueId = shortid.generate();
                    entity.uniqueId = uniqueId;
                    program.uniqueId = uniqueId;
                    return (
                      <EntityCard
                        className="entity-card-container"
                        entity={entity}
                        key={program.uniqueId}
                      />
                    );
                  })
            :
              <i>{T.translate('features.Overview.ProgramTab.emptyMessage')}</i>
          }
        </div>
      );
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
    }))
  }))
};
