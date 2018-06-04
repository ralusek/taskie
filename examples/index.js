'use strict';

const Promise = require('bluebird');
const Taskie = require('../src/index.js');


const sampleData = [
  {id: 1, value: 'Bread'},
  {id: 2, value: 'Banana'},
  {id: 3, value: 'Ginger'},
  {id: 4, value: 'Passionfruit'},
  {id: 5, value: 'Cereal'},
  {id: 6, value: 'Beef Jerky'},
  {id: 7, value: 'Pineapple'},
  {id: 8, value: 'Coconut'},
  {id: 9, value: 'Potato'},
  {id: 10, value: 'Peanut'},
  {id: 11, value: 'Bacon'},
  {id: 12, value: 'Scallop'},
  {id: 13, value: 'Halibut'},
  {id: 14, value: 'Spaghetti'},
  {id: 15, value: 'Lasagna'}
];


const indexed = sampleData.reduce((indexed, item) => {
  indexed[item.id] = item;
  return indexed;
}, {});


/**
 * Mock "fetch"
 */
function fetchItemFromData(id) {
  return new Promise((resolve) => {
    setTimeout(() => resolve(indexed[id]), Math.random() * 1000);
  });
}


/**
 * Mock "update"
 */
function updateItemInData(id, update) {
  return new Promise((resolve) => {
    setTimeout(() => resolve(Object.assign(indexed[id], update)));
  });
}





// Actual example code:

const example = {};

example.passive = new Taskie.Passive({
  handler: (currentId, pushNext, complete) => {
    const nextId = currentId + 3;
    if (indexed[nextId]) pushNext(nextId);
    if (nextId === 15) console.log('Completing early.', complete());
    return fetchItemFromData(currentId);
  },
  seed: [1, 2, 3],
  concurrency: 2
});


example.passive.start();

example.passive.onProgress((results) => {
  console.log('\n\nOn Progress:', results);
  console.log(example.passive.metrics);
  return Promise.map(results, result => updateItemInData(result.id, {tasty: true}))
  .tap(() => console.log('\nUpdated Data:\n', sampleData))
  .tap(() => new Promise((resolve) => setTimeout(() => resolve(), 2000)));
}, {batchSize: 3});


example.passive.onComplete()
.then(() => console.log('I completed.'))
.tap(() => console.log('\nFinal Data:\n', sampleData))
.catch(err => console.log('I errored', err))
.then(() => console.log('Final Metrics', example.passive.metrics));
