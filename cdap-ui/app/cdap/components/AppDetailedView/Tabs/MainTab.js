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
import EntityCard from 'components/EntityCard';
import {parseMetadata} from 'services/metadata-parser';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
require('./MainTab.less');

export default class MainTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      animationDirection: 'next'
    };
  }
  setAnimationDirection(direction) {
    this.setState({
      animationDirection : direction
    });
  }
  render() {
    return (
      <div className="app-detaliled-view-main-tab">
        <ReactCSSTransitionGroup
          component="div"
          transitionName={"entity-animation--" + this.state.animationDirection}
          transitionEnterTimeout={1000}
          transitionLeaveTimeout={1000}
        >
          {
            this.context.entity.programs.length ?
              this.context
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
                    return (
                      <EntityCard
                        poll={true}
                        className="entity-card-container"
                        entity={entity}
                        key={program.uniqueId}
                      />
                    );
                  })
            :
              <h3 className="text-center">
                <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
              </h3>
          }
        </ReactCSSTransitionGroup>
      </div>
    );
  }
}

MainTab.contextTypes = {
  entity: PropTypes.object
};
