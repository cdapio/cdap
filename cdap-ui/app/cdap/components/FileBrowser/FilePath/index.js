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
import {Link} from 'react-router-dom';
import classnames from 'classnames';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import {preventPropagation} from 'services/helpers';

require('./FilePath.scss');

const VIEW_LIMIT = 3;

const LinkWrapper = ({enableRouting, children, to, ...attributes}) => {
  if (enableRouting) {
    return (
      <Link
        to={to}
        {...attributes}
      >
        {children}
      </Link>
    );
  }
  return (
    <a
      href={to}
      {...attributes}
    >
      {children}
    </a>
  );
};

LinkWrapper.propTypes = {
  enableRouting: PropTypes.string,
  children: PropTypes.node,
  to: PropTypes.string
};

export default class FilePath extends Component {
  constructor(props) {
    super(props);

    this.state = {
      originalPath: '',
      paths: []
    };
    this.handlePropagation = this.handlePropagation.bind(this);
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

    let paths = [{
      id: shortid.generate(),
      name: 'Root',
      link: `${this.props.baseStatePath}/`
    }];

    splitPath.forEach((value, index) => {
      let directoryPath = splitPath.slice(0, index+1).join('/');

      let link = this.props.baseStatePath;
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

  handlePropagation(fullPath, e) {
    if (!this.props.enableRouting) {
      preventPropagation(e);
      if (this.props.onPathChange && typeof this.props.onPathChange === 'function') {
        let path = fullPath.slice(this.props.baseStatePath.length);
        this.props.onPathChange(path);
        return false;
      }
    }
  }

  renderCollapsedDropdown(collapsedLinks) {
    return (
      <div className="collapsed-dropdown">
        <UncontrolledDropdown
          className="collapsed-dropdown-toggle"
        >
          <DropdownToggle>
            ...
          </DropdownToggle>
          <DropdownMenu>
            {
              collapsedLinks.map((path) => {
                return (
                  <DropdownItem
                    title={path.name}
                  >
                    <LinkWrapper
                      key={path.id}
                      to={path.link}
                      onClick={this.handlePropagation.bind(this, path.link)}
                      enableRouting={this.props.enableRouting}
                    >
                      {path.name}
                    </LinkWrapper>
                  </DropdownItem>
                );
              })
            }
          </DropdownMenu>
        </UncontrolledDropdown>
      </div>
    );
  }

  renderBreadcrumb(links) {
    return (
      <div className="paths">
        {
          links.map((path, index) => {
            return (
              <LinkWrapper
                key={path.id}
                to={path.link}
                className={classnames({'active-directory': index === links.length - 1})}
                onClick={this.handlePropagation.bind(this, path.link)}
                enableRouting={this.props.enableRouting}
              >
                {path.name}
                {
                  index !== links.length - 1 ? <span className="path-divider">/</span> : null
                }
              </LinkWrapper>
            );
          })
        }
      </div>
    );
  }

  renderCollapsedView() {
    let splitIndex = this.state.paths.length - VIEW_LIMIT + 1;

    let collapsedLinks = this.state.paths.slice(0, splitIndex);
    let displayLinks = this.state.paths.slice(splitIndex);

    const collapsed = this.renderCollapsedDropdown(collapsedLinks);
    const breadcrumb = this.renderBreadcrumb(displayLinks);

    return (
      <div className="collapsed-paths">
        {collapsed}
        <span className="path-divider">/</span>
        {breadcrumb}
      </div>
    );
  }

  render() {
    return (
      <div className="file-path-container">
        {
          this.state.paths.length > VIEW_LIMIT ?
            this.renderCollapsedView()
          :
            this.renderBreadcrumb(this.state.paths)
        }
      </div>
    );
  }
}

FilePath.propTypes = {
  baseStatePath: PropTypes.string,
  fullpath: PropTypes.string,
  enableRouting: PropTypes.bool,
  onPathChange: PropTypes.func
};
