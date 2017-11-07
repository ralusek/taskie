'use strict';

const Promise = require('bluebird');



/**
 *
 */
class ProgressHandler {
  /**
   *
   */
  constructor(handler, {name, batchSize}) {
    // The progress handler's name. (optional)
    this.name = name;

    // The amount of progress values to amass in the backlog prior to clearing
    // and calling the handler.
    this.batchSize = batchSize || 1;

    // The backlog of progress items which are pooled on progress until the
    // specified batchSize is achieved, at which point the backlog is cleared
    // and the handler is called.
    this.backlog = [];

    // The progress handler.
    this.handler = handler;
  }


  /** 
   * Add progress value to progress backlog.
   */
  progress(value) {
    this.backlog.push(value);

    return Promise.resolve(evaluateBacklog(this));
  }


  /**
   *
   */
  drain() {
    return Promise.resolve(evaluateBacklog(this, {force: true}));
  }
}


/**
 * Evaluates the progress handler's backlog. If the size of the backlog has
 * arrived at the configured batchSize, the backlog is reset and the progress
 * handler is reset.
 */
function evaluateBacklog(progressHandler, {force = false} = {}) {
  const {
    backlog,
    batchSize,
    handler
  } = progressHandler;

  const length = backlog.length;
  if ((length >= batchSize) || force) {
    // Clear current backlog.
    progressHandler.backlog = [];

    // Call the progress handler. If batchSize is 1, call with singular backlog
    // item, else pass in array.
    return handler(batchSize === 1 ? backlog[0] : backlog);
  }
}



/**
 *
 */
module.exports = ProgressHandler;
