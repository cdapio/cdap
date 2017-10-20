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

import shortid from 'shortid';
import FilePath from 'components/FileBrowser/FilePath';
import {connect} from 'react-redux';
import {setPrefix} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';

class S3Path extends FilePath {
  constructor(props) {
    super(props);
  }
  processPath(props) {
    let path = props.fullpath;
    if (path[path.length -1 ] === '/') {
      path = path.slice(0, path.length - 1);
    }
    let parts = path.split(/\//);
    let individualPaths = [];
    let pathname = window.location.pathname.replace(/\/cdap/, '');
    if (path === '/') {
      individualPaths.push({
        name: 'Root',
        id: shortid.generate(),
        link: `${pathname}?prefix=/`
      });
    } else {
      parts.forEach((part, i) => {
        let newPath = parts.slice(0, i + 1).join('/');
        if (part === '') {
          newPath = '/';
          individualPaths.push({
            id: shortid.generate(),
            name: 'Root',
            link: `${pathname}?prefix=${newPath}`,
          });
          return;
        }
        newPath = newPath[newPath.length - 1] !== '/' ? `${newPath}/` : newPath;
        individualPaths.push({
          id: shortid.generate(),
          link: `${pathname}?prefix=${newPath}`,
          name: part
        });
      });
    }
    this.setState({
      paths: individualPaths,
      originalPath: path
    });
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    ...ownProps,
    fullpath: state.s3.prefix
  };
};
const mapDispatchToProps = () => {
  return {
    onPathChange: (path) => {
      // The difference between basepath and link we provide is ?prefix=/folder1/folder2
      // split with '=' and the second element in the path we need.
      let p = path.split('=').pop();
      p = p[p.length - 1] !== '/' ? `${p}/` : p;
      setPrefix(p);
    }
  };
};

const S3PathWrapper = connect(mapStateToProps, mapDispatchToProps)(S3Path);

export default S3PathWrapper;
