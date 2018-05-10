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

    p(this).concurrency = config.concurrency || 1;
    p(this).autoComplete = config.autoComplete !== false;
    p(this).strict = config.strict !== false;

    p(this).progressHandlers = [];
    p(this).errorHandlers = [];

    p(this).onCompleteDeferral;
    p(this).onCompletePromise = new Promise((resolve, reject) => p(this).onCompleteDeferral = {resolve, reject});

    p(this).state = {
      isPaused: false,
      errors: [],
      handling: {
        handling: 0,
        resolved: 0,
        errored: 0,
        totalHandled: 0,
        maxConcurrency: 0,
        totalTimeHandling: 0,
        maxTimeHandling: 0,
        averageTimeHandling: 0
      },
      progressHandlers: {
        handling: 0,
        resolved: 0,
        errored: 0,
        totalHandled: 0,
        totalTimeHandling: 0,
        maxTimeHandling: 0,
        averageTimeHandling: 0
      },
      queue: {
        length: 0,
        maxLength: 0,
        totalQueuedCount: 0,
        totalTimeQueued: 0,
        maxTimeQueued: 0,
        averageTimeQueued: 0
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
  get metrics () {
    const {totalTimeHandling: omitA, ...handling} = p(this).state.handling;
    const {totalTimeHandling: omitB, ...progressHandlers} = p(this).state.progressHandlers;
    const {totalTimeQueued, ...queued} = p(this).state.queue;
    return { handling, progressHandlers, queued };
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
    // Do nothing if already handling at concurrency limit.
    if (p(this).state.handling.handling >= p(this).concurrency) return;

    
    if (!p(this).queue.length) {
      if ((this.constructor === Passive) &&
          p(this).autoComplete &&
          !p(this).state.handling.handling) complete(this);
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
  if (p(taskie).strict && p(taskie).state.errors.length) throw new Error('Cannot push payload to taskie, taskie is in an error state.');
  if (p(taskie).state.isComplete) throw new Error('Cannot push payload to taskie, taskie is in completed state.');

  let deferred;
  const promise = new Promise((resolve, reject) => deferred = {resolve, reject});
  p(taskie).queue.push({payload, callback, deferred, queuedAt: Date.now()});

  const queueState = p(taskie).state.queue;
  queueState.totalQueuedCount++;
  queueState.length = p(taskie).queue.length;
  if (queueState.length > queueState.maxLength) queueState.maxLength = queueState.length;

  taskie.refresh();

  return promise;
}


/**
 *
 */
function resolveNext(taskie) {
  const current = p(taskie).queue.shift();

  // Update queue metrics.
  const timeInQueue = Date.now() - current.queuedAt;

  const queueState = p(taskie).state.queue;
  const totalDequeued = queueState.totalQueuedCount - queueState.length--;

  queueState.totalTimeQueued += timeInQueue;
  if (timeInQueue > queueState.maxTimeQueued) queueState.maxTimeQueued = timeInQueue;
  queueState.averageTimeQueued = queueState.totalTimeQueued / totalDequeued;

  // Begin Handling metrics
  const handlingState = p(taskie).state.handling;
  if (++handlingState.handling > handlingState.maxConcurrency) handlingState.maxConcurrency = handlingState.handling;
  
  const beginHandling = Date.now();

  Promise.resolve(p(taskie).handler(
    current.payload,
    // We pass in the function to push the next item. This is done here because
    // for Passive taskies, the `push` method is not exposed as a method. The
    // only way to push into the taskie's queue is therefore through this
    // callback on the handler.
    p(taskie).pushCallbacks.pushNext,
    p(taskie).pushCallbacks.completed
  ))
  // Update Handling metrics.
  .tap(() => updateHandlingMetricsOnComplete(handlingState, beginHandling, false))
  .tapCatch(() => updateHandlingMetricsOnComplete(handlingState, beginHandling, true))

  .then((response) => {
    current.callback && current.callback(null, response); // Don't wait for progress handlers to do callback.

    // Begin Progress Handler metrics.
    const beginProgressHandling = Date.now();
    const progressHandlingState = p(taskie).state.progressHandlers;
    progressHandlingState.handling++;
    
    // Wait for progress handlers to complete to call promise.
    return manageProgressHandlers(taskie, response)
    // Update Progress Handler metrics
    .tap(() => updateHandlingMetricsOnComplete(progressHandlingState, beginProgressHandling, false))
    .tapCatch(() => updateHandlingMetricsOnComplete(progressHandlingState, beginProgressHandling, true))
    .then(() => current.deferred.resolve(response));
  })
  .catch((err) => {
    current.callback && current.callback(err); // Don't wait for error handlers to do callback.

    return manageErrorHandlers(taskie, err)
    .then(() => current.deferred.reject(err));
  })
  .finally(() => {
    

    taskie.refresh();
  });
}


/**
 *
 */
function manageProgressHandlers(taskie, response) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    return progressHandler.progress(response)
    // Catch and handle any error.
    .catch(err => handleErrorState(taskie, err, {
      type: 'progress',
      drain: false,
      name: progressHandler.name,
      error: err
    }));
  });
}


/**
 *
 */
function drainProgressHandlers(taskie) {
  return Promise.map(p(taskie).progressHandlers, progressHandler => {
    return progressHandler.drain()
    // Catch and handle any error.
    .catch(err => handleErrorState(taskie, err, {
      type: 'progress',
      drain: true,
      name: progressHandler.name,
      error: err
    }));
  });
}


/**
 *
 */
function manageErrorHandlers(taskie, err) {
  const errorHandlers = p(taskie).errorHandlers;

  if (!errorHandlers.length) return Promise.resolve(handleErrorState(taskie, err, {
    type: 'unhandledError',
    error: err
  }));

  return Promise.map(p(taskie).errorHandlers, errorHandler => {
    return Promise.resolve(errorHandler.handler(err))
    // Catch and handle any error.
    .catch(err => handleErrorState(taskie, err, {
      type: 'error',
      error: err
    }));
  });
}


/**
 *
 */
function handleErrorState(taskie, error, meta) {
  p(taskie).state.errors.push({error, meta});

  if (p(taskie).strict && !p(taskie).state.isComplete) complete(taskie, {error, meta});
}



/**
 *
 */
function complete(taskie, err) {
  if (p(taskie).state.isComplete) return Promise.reject(new Error('Cannot call complete on taskie, already completed.'));
  p(taskie).state.isComplete = true;

  return drainProgressHandlers(taskie)
  .then(() => err ? p(taskie).onCompleteDeferral.reject(err) : p(taskie).onCompleteDeferral.resolve());
}


/**
 *
 */
function updateHandlingMetricsOnComplete(state, startedAt, isError) {
  const timeHandling = Date.now() - startedAt;

  state.handling--;
  state.totalHandled++;
  isError ? state.errored++ : state.resolved++;

  state.totalTimeHandling += timeHandling;
  if (timeHandling > state.maxTimeHandling) state.maxTimeHandling = timeHandling;
  state.averageTimeHandling = state.totalTimeHandling / state.totalHandled;
}



/**
 *
 */
module.exports = Object.freeze({Active, Passive});
