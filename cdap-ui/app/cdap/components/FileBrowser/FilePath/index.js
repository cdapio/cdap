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

import React, { Component, PropTypes } from 'react';
import shortid from 'shortid';
import {Link} from 'react-router';
import classnames from 'classnames';

require('./FilePath.scss');

export default class FilePath extends Component {
  constructor(props) {
    super(props);

    this.state = {
      originalPath: '',
      paths: []
    };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.originalPath === nextProps.fullpath) { return; }

    this.processPath(nextProps);
  }

  processPath(props) {
    let splitPath = props.fullpath.split('/')
      .filter((directory) => {
        return directory.length > 0;
      });

    let paths = [];

    splitPath.forEach((value, index) => {
      let link = this.props.baseStatePath;

      let directoryPath = splitPath.slice(0, index+1).join('/');

      link = `${link}/${directoryPath}`;

      paths.push({
        id: shortid.generate(),
        name: value,
        link
      });
    });

    this.setState({
      paths,
      originalPath: props.fullpath
    });
  }

  render() {
    return (
      <div className="file-path-container">
        {
          this.state.paths.map((path, index) => {
            return (
              <Link
                key={path.id}
                to={path.link}
                className={classnames({'active-directory': index === this.state.paths.length - 1})}
              >
                {path.name}
                {
                  index !== this.state.paths.length - 1 ? <span className="fa fa-chevron-right path-divider"></span> : null
                }
              </Link>

            );
          })
        }
      </div>
    );
  }
}

FilePath.propTypes = {
  baseStatePath: PropTypes.string,
  fullpath: PropTypes.string
};
