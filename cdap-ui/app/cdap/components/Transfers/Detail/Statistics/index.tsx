/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Stat from './Stat';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px',
      display: 'grid',
      gridTemplateColumns: '1fr 1fr 1fr 1fr 1fr',
    },
  };
};

function generateRandom(seed, range) {
  return Math.ceil(Math.random() * range) + seed;
}

function randomTime() {
  return generateRandom(1000, 4000);
}

const StatisticsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [insert, setInsert] = React.useState(4209);
  const [update, setUpdate] = React.useState(7233);
  const [del, setDelete] = React.useState(964);
  const [upsert, setUpsert] = React.useState(63);
  const [error, setError] = React.useState(1);

  function addInsert() {
    setTimeout(() => {
      const val = generateRandom(1, 50);
      setInsert(insert + val);
    }, randomTime());
  }
  function addUpdate() {
    setTimeout(() => {
      const val = generateRandom(1, 76);
      setUpdate(update + val);
    }, randomTime());
  }
  function addDelete() {
    setTimeout(() => {
      const val = generateRandom(1, 35);
      setDelete(del + val);
    }, randomTime());
  }
  function addUpsert() {
    setTimeout(() => {
      const val = generateRandom(1, 10);
      setUpsert(upsert + val);
    }, randomTime());
  }

  React.useEffect(addInsert, [insert]);
  React.useEffect(addUpdate, [update]);
  React.useEffect(addDelete, [del]);
  React.useEffect(addUpsert, [upsert]);

  return (
    <React.Fragment>
      <h3 className="pl-5 pt-3">Events Last Hour</h3>
      <div className={classes.root}>
        <Stat type="INSERT" stat={insert} />
        <Stat type="UPDATE" stat={update} />
        <Stat type="UPSERT" stat={upsert} />
        <Stat type="DELETE" stat={del} />
        <Stat type="ERROR" stat={error} />
      </div>
    </React.Fragment>
  );
};

const Statistics = withStyles(styles)(StatisticsView);
export default Statistics;
