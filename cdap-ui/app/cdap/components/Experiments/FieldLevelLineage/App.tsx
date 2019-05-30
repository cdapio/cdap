import React from 'react';
import './App.css';
import Table from './Table'; // TO DO: Look into SortableStickyGrid instead of Table
import Header from './Header';
import { Consumer } from './FllContext';

function App() {
  return (
    <Consumer>
      {({ causeSets, target, targetFields, impactSets, firstCause, firstImpact, firstField }) => {
        return (
          <div className="fll-container">
            <div className="cause-col">
              <div className="cause-header" />
              <Header type="cause" first={firstCause} total={Object.keys(causeSets).length} />
              <div className="cause-tables col">
                {Object.keys(causeSets).map((key) => {
                  return <Table tableName={key} key={`cause ${key}`} nodes={causeSets[key]} />;
                })}
              </div>
            </div>
            <div className="target-col">
              <Header type="target" first={firstField} total={Object.keys(targetFields).length} />
              <div className="target-table col">
                <Table tableName={target} key="target" nodes={targetFields} />
              </div>
            </div>
            <div className="impact-col">
              <Header type="target" first={firstImpact} total={Object.keys(impactSets).length} />
              <div className="impact-tables col">
                {Object.keys(impactSets).map((key) => {
                  return <Table tableName={key} key={`impact ${key}`} nodes={impactSets[key]} />;
                })}
              </div>
            </div>
          </div>
        );
      }}
    </Consumer>
  );
}

export default App;
