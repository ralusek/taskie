'use strict';

const Promise = require('bluebird');


// This establishes a private namespace.
const namespace = new WeakMap();
function p(object) {
  if (!namespace.has(object)) namespace.set(object, {});
  return namespace.get(object);
}


/**
 *
 */
class Passive {
  constructor(config) {
    config = config || {};

    if (!config.handler) throw new Error('Taskie require a handler function to be provided.');
    p(this).handler = config.handler;

    p(this).queue = [];

    p(this).autoComplete = config.autoComplete !== false;


    p(this).concurrency = config.concurrency || 1;

    p(this).progressHandlers = [];
    p(this).errorHandlers = [];

    p(this).onCompleteDeferral;
    p(this).onCompletePromise = new Promise((resolve, reject) => p(this).onCompleteDeferral = {resolve, reject});

    p(this).state = {
      isPaused: false,
      isErrored: false,
      resolvingCount: 0,
      handlingProgressCount: 0,
      handlingErrorCount: 0
    };


    p(this).pushCallbacks = Object.freeze({
      pushNext: (nextPayload) => push(this, nextPayload),
      completed: (err) => complete(this, err)
    });


    // Init with seed.
    if (config.seed) config.seed.forEach(seed => push(this, seed));
  }


  /**
   * 
   */
  onProgress(handler, config) {
    config = config || {};

    p(this).progressHandlers.push({
      handler,
      batchSize: config.batchSize || 1,
      backlog: []
    });
  }


  /**
   * 
   */
  onError(handler, config) {
    config = config || {};

    p(this).errorHandlers.push({handler});
  }


  /**
   *
   */
  onComplete() {
    return p(this).onCompletePromise;
  }


  /**
   *
   */
  refresh() {
    if (p(this).state.isComplete) return;
    // Do nothing if paused.
    if (p(this).state.isPaused) return;
    // Do nothing if already resolving at concurrency limit.
    if (p(this).state.resolvingCount >= p(this).concurrency) return;
    if (p(this).state.handlingProgressCount) return;
    if (p(this).state.handlingErrorCount) return;

    
    if (!p(this).queue.length) {
      if ((this.constructor === Passive) &&
          p(this).autoComplete &&
          !p(this).state.resolvingCount &&
          !p(this).state.handlingProgressCount &&
          !p(this).state.handlingErrorCount) complete(this);
      // Do nothing if there is nothing in the queue.
      return;
    }


    resolveNext(this);

    this.refresh();
  }


  /**
   *
   */
  pause() {
    p(this).state.isPaused = true;
  }


  /**
   *
   */
  start() {
    p(this).state.isPaused = false;
    this.refresh();
  }
}



/**
 *
 */
class Active extends Passive {
  constructor(config) {
    super(config);
    this.start();
  }

  push(payload) {
    return push(this, payload);
  }
}



/**
 * Push is a private method by default (to a Passive taskie). A non-Passive taskie
 * will expose its own push method to call this.
 */
function push(taskie, payload, callback) {
  if (p(taskie).state.isErrored) throw new Error('Cannot push payload to taskie, taskie is in an error state.');
  if (p(taskie).state.isComplete) throw new Error('Cannot push payload to taskie, taskie is in completed state.');

  p(taskie).queue.push({payload, callback});
  taskie.refresh();
}


/**
 *
 */
function resolveNext(taskie) {
  const current = p(taskie).queue.shift();

  p(taskie).state.resolvingCount++;
  
  return Promise.resolve(p(taskie).handler(
    current.payload,
    // We pass in the function to push the next item. This is done here because
    // for Passive taskies, the `push` method is not exposed as a method. The
    // only way to push into the taskie's queue is therefore through this
    // callback on the handler.
    p(taskie).pushCallbacks.pushNext,
    p(taskie).pushCallbacks.completed
  ))
  .finally(() => p(taskie).state.resolvingCount--)
  .then((response) => {
    manageProgressHandlers(taskie, response);
    current.callback && current.callback(null, response); // Don't wait for progress handlers.
  })
  .catch((err) => {
    manageErrorHandlers(taskie, err);
    current.callback && current.callback(err); // Don't wait for error handlers.
  })
  .finally(() => taskie.refresh());
}


/**
 *
 */
function manageProgressHandlers(taskie, response) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    p(taskie).state.handlingProgressCount++;
    progressHandler.backlog.push(response);
    const backlog = progressHandler.backlog.slice();
    if (backlog.length >= progressHandler.batchSize) {
      progressHandler.backlog = []; // Clear existing.
      return Promise.resolve(progressHandler.handler(progressHandler.batchSize > 1 ? backlog : backlog[0]))
      .finally(() => p(taskie).state.handlingProgressCount--);
    }
    p(taskie).state.handlingProgressCount--;
  })
  .catch(err => handleErrorState(taskie, err))
  .finally(() => taskie.refresh());
}


/**
 *
 */
function drainProgressHandlers(taskie) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    const backlog = progressHandler.backlog.slice();
    if (!backlog.length) return;
    return progressHandler.handler(progressHandler.batchSize > 1 ? backlog : backlog[0]);
  })
  .catch(err => handleErrorState(taskie, err));
}


/**
 *
 */
function manageErrorHandlers(taskie, err) {
  const errorHandlers = p(taskie).errorHandlers;

  if (!errorHandlers) var promise = Promise.reject(err);
  // If any error handler rejects, this whole thing will reject.
  else promise = Promise.map(p(taskie).errorHandlers, errorHandler => {
    p(taskie).state.handlingErrorCount++;
    return Promise.resolve(errorHandler.handler(err))
    .finally(() => p(taskie).state.handlingErrorCount--)
  });

  return promise
  .catch(err => handleErrorState(taskie, err))
  .finally(() => taskie.refresh());
}


/**
 *
 */
function handleErrorState(taskie, err) {
  p(taskie).state.isErrored = true;
  p(taskie).state.error = p(taskie).state.error || err;

  p(taskie).queue.forEach(item => item.callback && item.callback(new Error('Taskie queue rejected because queue has entered error state.')));
  p(taskie).queue = [];

  complete(taskie, err);
}



/**
 *
 */
function complete(taskie, err) {
  p(taskie).state.isComplete = true;

  return drainProgressHandlers(taskie)
  .then(() => err ? p(taskie).onCompleteDeferral.reject(err) : p(taskie).onCompleteDeferral.resolve());
}



/**
 *
 */
module.exports = Object.freeze({Active, Passive});
