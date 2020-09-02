/*
 * Copyright © 2020 Cask Data, Inc.
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

import React, { useState, useEffect } from 'react';
import VirtualScroll from './index';
import withStyles from '@material-ui/core/styles/withStyles';
import FormGroup from '@material-ui/core/FormGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Switch from '@material-ui/core/Switch';

const styles = () => {
  return {
    container: {
      marginTop: '20px',
    },
    root: {
      height: 30,
      lineHeight: '30px',
      display: 'flex',
      justifyContent: 'space-between',
      padding: '0 10px',
    },
    row: {
      lineHeight: '20px',
      background: 'hotpink',
      maxWidth: '200px',
      margin: '0 10px',
      boxShadow: '0 0 1px 0 rgba(0, 0, 0, 0.5)',
    },
    checkbox: {
      padding: '0 10px',
    },
  };
};

const Item = ({ index, classes }): React.ReactElement => (
  <div className={`${classes.root} ${classes.row}`} key={index}>
    <strong>row index {index}</strong>
  </div>
);
const StyledItem = withStyles(styles)(Item);
let myList = new Array(100).fill(null);
let promise;
function renderList(checked, visibleNodeCount, startNode) {
  if (checked) {
    if (promise) {
      return promise;
    }
    let newList = myList.slice(startNode, startNode + visibleNodeCount);
    newList = newList.map((_, index) => (
      <StyledItem key={index + startNode} index={index + startNode} />
    ));
    if (newList.length < visibleNodeCount) {
      promise = new Promise((resolve) => {
        setTimeout(() => {
          myList = myList.concat(new Array(100).fill(null));
          resolve(
            myList
              .slice(startNode, startNode + visibleNodeCount)
              .map((_, index) => <StyledItem key={index + startNode} index={index + startNode} />)
          );
          promise = null;
        }, 2000);
      });
    }
    return newList;
  }
  return myList
    .slice(startNode, startNode + visibleNodeCount)
    .map((_, index) => <StyledItem key={index + startNode} index={index + startNode} />);
}

function App({ classes }) {
  const [checked, setChecked] = useState(true);
  useEffect(() => {
    if (!checked) {
      myList = new Array(100000).fill(null);
    } else {
      myList = new Array(100).fill(null);
    }
  }, [checked]);
  return (
    <div className={classes.container}>
      <FormGroup>
        <FormControlLabel
          className={classes.checkbox}
          control={
            <Switch
              checked={checked}
              onChange={() => setChecked(!checked)}
              name="promisify"
              color="primary"
            />
          }
          label="Promisify"
        />
      </FormGroup>
      <VirtualScroll
        itemCount={() => myList.length}
        visibleChildCount={20}
        childHeight={30}
        renderList={renderList.bind(null, checked)}
        childrenUnderFold={5}
      />
    </div>
  );
}

export default withStyles(styles)(App);
