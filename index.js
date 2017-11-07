'use strict';

const Promise = require('bluebird');

const ProgressHandler = require('./progress-handler');


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

    if ((this.constructor === Passive) && !config.handler) throw new Error('Taskie require a handler function to be provided.');
    p(this).handler = config.handler || ((payload, pushNext) => payload);

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
      count: {
        resolving: 0,
        resolved: 0,
        errored: 0
      }
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

    p(this).progressHandlers.push(new ProgressHandler(handler, config));
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
    if (p(this).state.count.resolving >= p(this).concurrency) return;

    
    if (!p(this).queue.length) {
      if ((this.constructor === Passive) &&
          p(this).autoComplete &&
          !p(this).state.count.resolving) complete(this);
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

  push(payload, callback) {
    return push(this, payload, callback);
  }

  complete(err) {
    return complete(this, err);
  }
}



/**
 * Push is a private method by default (to a Passive taskie). A non-Passive taskie
 * will expose its own push method to call this.
 *
 * Returns a promise, as well as accepts a callback.
 */
function push(taskie, payload, callback) {
  if (p(taskie).state.isErrored) throw new Error('Cannot push payload to taskie, taskie is in an error state.');
  if (p(taskie).state.isComplete) throw new Error('Cannot push payload to taskie, taskie is in completed state.');

  let deferred;
  const promise = new Promise((resolve, reject) => deferred = {resolve, reject});
  p(taskie).queue.push({payload, callback, deferred});
  taskie.refresh();

  return promise;
}


/**
 *
 */
function resolveNext(taskie) {
  const current = p(taskie).queue.shift();

  p(taskie).state.count.resolving++;
  
  Promise.resolve(p(taskie).handler(
    current.payload,
    // We pass in the function to push the next item. This is done here because
    // for Passive taskies, the `push` method is not exposed as a method. The
    // only way to push into the taskie's queue is therefore through this
    // callback on the handler.
    p(taskie).pushCallbacks.pushNext,
    p(taskie).pushCallbacks.completed
  ))
  .then((response) => {
    current.callback && current.callback(null, response); // Don't wait for progress handlers to do callback.

    // Wait for progress handlers to complete to call promise.
    return manageProgressHandlers(taskie, response)
    .then(() => current.deferred.resolve(response));
  })
  .catch((err) => {
    current.callback && current.callback(err); // Don't wait for error handlers to do callback.

    return manageErrorHandlers(taskie, err)
    .then(() => current.deferred.reject(err));
  })
  .finally(() => {
    p(taskie).state.count.resolving--;
    taskie.refresh();
  });
}


/**
 *
 */
function manageProgressHandlers(taskie, response) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    return progressHandler.progress(response);
  })
  .catch(err => handleErrorState(taskie, err));
}


/**
 *
 */
function drainProgressHandlers(taskie) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    return progressHandler.drain();
  })
  .catch(err => handleErrorState(taskie, err));
}


/**
 *
 */
function manageErrorHandlers(taskie, err) {
  const errorHandlers = p(taskie).errorHandlers;

  let promise;
  if (!errorHandlers) promise = Promise.reject(err);
  // If any error handler rejects, this whole thing will reject.
  else promise = Promise.map(p(taskie).errorHandlers, errorHandler => {
    return Promise.resolve(errorHandler.handler(err));
  });

  return promise
  .catch(err => handleErrorState(taskie, err));
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
