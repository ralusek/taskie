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

    if (config.seed) config.seed.forEach(seed => push(this, seed));


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
    // Do nothing if there is nothing in the queue.
    if (!p(this).queue.length) return;


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
  push(payload) {
    return push(this, payload);
  }
}



/**
 * Push is a private method by default (to a Passive taskie). A non-Passive taskie
 * will expose its own push method to call this.
 */
function push(taskie, payload) {
  let deferred;
  const promise = new Promise((resolve, reject) => deferred = {resolve, reject});

  p(taskie).queue.push({payload, deferred});

  return promise;
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
    current.deferred.resolve(response); // Don't wait for progress handlers.
  })
  .catch((err) => {
    manageErrorHandlers(taskie, err);
    current.deferred.reject(err); // Don't wait for error handlers.
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
      return Promise.resolve(progressHandler.handler(backlog.length > 1 ? backlog : backlog[0]))
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
function manageErrorHandlers(taskie, err) {
  // If any error handler rejects, this whole thing will reject.
  return Promise.map(p(taskie).errorHandlers, errorHandler => {
    p(taskie).state.handlingErrorCount++;
    return Promise.resolve(errorHandler.handler(err))
    .finally(() => p(taskie).state.handlingErrorCount--)
  })
  .catch(err => handleErrorState(taskie, err))
  .finally(() => taskie.refresh());
}


/**
 *
 */
function handleErrorState(taskie, err) {
  console.log(err);
  p(taskie).state.isErrored = true;
  p(taskie).state.error = p(taskie).state.error || err;

  p(taskie).queue.forEach(item => item.deferred.reject(new Error('Taskie queue rejected because queue has entered error state.')));
  p(taskie).queue = [];
}



/**
 *
 */
function complete(taskie, err) {
  p(taskie).state.isComplete = true;
  if (err) p(taskie).state.error = err;
  err ? p(taskie).onCompleteDeferral.reject(err) : p(taskie).onCompleteDeferral.resolve();
}



/**
 *
 */
module.exports = Object.freeze({Active, Passive});
