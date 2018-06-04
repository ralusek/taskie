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

    p(this).onceEveryHandlers = [];
    p(this).onceEveryProgressedHandlers = [];
    p(this).progressHandlers = [];
    p(this).errorHandlers = [];

    p(this).onCompleteDeferral;
    p(this).onCompletePromise = new Promise((resolve, reject) => p(this).onCompleteDeferral = {resolve, reject});

    p(this).state = {
      startedAt: null,
      isPaused: false,
      errors: [],
      handling: {
        handling: 0,
        resolved: 0,
        errored: 0,
        totalHandled: 0,
        handledPerSecond: 0,
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
        handledPerSecond: 0,
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
      complete: (options) => complete(this, options)
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
   * Registers a one time callback to be called on the next item to be handled.
   * Handler is NOT awaited to progress the queue (see onProgress).
   */
  onceEvery(handler) {
    p(this).onceEveryHandlers.push(handler);
  }


  /**
   *
   */
  onceEveryProgressed(handler) {
    p(this).onceEveryProgressedHandlers.push(handler);
  }


  /**
   * Registers an asynchronous handler that is either called every queue item,
   * or every batchSize if provided in config. Progress handlers hold up the
   * advancement of the queue while being resolved, so as to be used as the
   * mechanism for adding backpressure to the progression of a job.
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
    // Throw error if terminated.
    if (p(this).state.isTerminated) throw new Error('Cannot refresh Taskie, is terminated.');
    // Do nothing if paused.
    if (p(this).state.isPaused) return;
    // Do nothing if completed but not actively draining queue.
    if (p(this).state.isComplete && !p(this).state.isDrainingQueue) return;
    // Do nothing if already handling at concurrency limit.
    const totalHandling = p(this).state.handling.handling + p(this).state.progressHandlers.handling;
    if (totalHandling >= p(this).concurrency) return;

    
    if (!p(this).queue.length) {
      if ((this.constructor === Passive) &&
          !p(this).state.isComplete &&
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
    if (p(this).state.startedAt) return;
    p(this).state.startedAt = Date.now();
    p(this).state.isPaused = false;
    this.refresh();
  }


  /**
   *
   */
  terminate() {
    if (p(this).state.isTerminated) throw new Error('Cannot call terminate on taskie, already terminated.');
    p(this).state.isTerminated = true;
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

  complete(options) {
    return complete(this, options);
  }
}



/**
 * Push is a private method by default (to a Passive taskie). A non-Passive taskie
 * will expose its own push method to call this.
 *
 * Returns a promise, as well as accepts a callback.
 */
function push(taskie, payload, callback) {
  if (p(taskie).state.isTerminated) throw new Error('Cannot push payload to taskie, taskie is terminated.');
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
    p(taskie).pushCallbacks.complete
  ))
  // Update Handling metrics.
  .tap(() => updateHandlingMetricsOnComplete(taskie, 'handling', beginHandling, false))
  .tapCatch(() => updateHandlingMetricsOnComplete(taskie, 'handling', beginHandling, true))

  .then((response) => {
    // Stop execution if terminated.
    if (p(taskie).state.isTerminated) return;
    current.callback && current.callback(null, response); // Don't wait for progress handlers to do callback.
    manageOnceEveryHandlers(taskie, response); // Don't wait for progress handlers to manage onceEvery handlers.

    // Stop execution if terminated (could have been terminated within callback).
    if (p(taskie).state.isTerminated) return;
    // Begin Progress Handler metrics.
    const beginProgressHandling = Date.now();
    const progressHandlingState = p(taskie).state.progressHandlers;
    progressHandlingState.handling++;
    
    // Wait for progress handlers to complete to call promise.
    return manageProgressHandlers(taskie, response)
    // Update Progress Handler metrics
    .tap(() => updateHandlingMetricsOnComplete(taskie, 'progressHandlers', beginProgressHandling, false))
    .tapCatch(() => updateHandlingMetricsOnComplete(taskie, 'progressHandlers', beginProgressHandling, true))
    .then(() => {
      manageOnceEveryProgressedHandlers(taskie, response); // Don't wait for handlers.
      return current.deferred.resolve(response);
    });
  })
  .catch((err) => {
    current.callback && current.callback(err); // Don't wait for error handlers to do callback.

    return manageErrorHandlers(taskie, err)
    .then(() => current.deferred.reject(err));
  })
  .finally(() => (!p(taskie).state.isTerminated) && taskie.refresh());
}


/**
 *
 */
function manageOnceEveryHandlers(taskie, response) {
  const handlers = p(taskie).onceEveryHandlers;
  p(taskie).onceEveryHandlers = [];

  handlers.forEach(handler => handler(response));
}

/**
 *
 */
function manageOnceEveryProgressedHandlers(taskie, response) {
  const handlers = p(taskie).onceEveryProgressedHandlers;
  p(taskie).onceEveryProgressedHandlers = [];

  handlers.forEach(handler => handler(response));
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
  })
  .then(() => {
    return new Promise((resolve, reject) => {
      testForCompletion();
      function testForCompletion() {
        if (!p(taskie).state.progressHandlers.handling) return resolve();
        taskie.onceEveryProgressed(() => testForCompletion());
      }
    });
  });
}


/**
 *
 */
function drainQueue(taskie) {
  p(taskie).state.isDrainingQueue = true;

  return new Promise((resolve, reject) => {
    // Start in case it has been paused.
    taskie.start();

    testForCompletion();
    function testForCompletion() {
      if (!(p(taskie).queue.length || p(taskie).state.handling.handling)) return resolve();
      taskie.onceEvery(() => testForCompletion());
    }
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
function complete(taskie, {willDrainQueue = true, willDrainProgressHandlers = true} = {}, err) {
  if (p(taskie).state.isComplete) return Promise.reject(new Error('Cannot call complete on taskie, already completed.'));
  p(taskie).state.isComplete = true;

  return Promise.resolve(willDrainQueue && drainQueue(taskie))
  .then(() => willDrainProgressHandlers && drainProgressHandlers(taskie))
  .then(() => err ? p(taskie).onCompleteDeferral.reject(err) : p(taskie).onCompleteDeferral.resolve());
}



/**
 *
 */
function updateHandlingMetricsOnComplete(taskie, stateKey, itemStartedAt, isError) {
  const now = Date.now();
  const state = p(taskie).state[stateKey];

  const totalRuntime = now - p(taskie).state.startedAt;

  const timeHandlingItem = now - itemStartedAt;

  state.handling--;
  state.totalHandled++;
  isError ? state.errored++ : state.resolved++;
  state.handledPerSecond = state.totalHandled / (totalRuntime / 1000);

  state.totalTimeHandling += timeHandlingItem;
  if (timeHandlingItem > state.maxTimeHandling) state.maxTimeHandling = timeHandlingItem;
  state.averageTimeHandling = state.totalTimeHandling / state.totalHandled;
}



/**
 *
 */
module.exports = Object.freeze({Active, Passive});
