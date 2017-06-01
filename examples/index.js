'use strict';

const Taskie = require('../');

const thing = new Taskie.Passive({
  handler: (queueItem, pushNext) => {
    console.log('I am processing a queue item', queueItem);

    return new Promise((resolve) => {
      if (queueItem.startAt > 0.1) pushNext({startAt: Math.random()});
      resolve('I am some result');
    });
  },
  seed: [{startAt: Math.random()}, {startAt: Math.random()}, {startAt: Math.random()}],
  concurrency: 2
});


thing.start();

thing.onProgress((sup) => {
  console.log('on progress', sup);
  return new Promise((resolve) => {
    setTimeout(() => resolve(), Math.random() * 3000);
  });
}, {batchSize: 3});
