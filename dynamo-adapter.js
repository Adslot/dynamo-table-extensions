const { promisify } = require('util');
const { DynamoTable } = require('./slim');

const isCallback = (fn) => typeof fn === 'function';

const validateCall = (methodName, args, minLength) => {
  if (args.length < minLength)
    throw new Error(`Incorrect args supplied to method ${methodName}: expected: ${minLength} got: ${args.length}`);
};

/**
  Provides an adapter for the DynamoTable class to allow for promisified
  api usage when omitting the cb parameter.
*/
class DynamoAdapter {
  constructor(name, options) {
    this._delegate = new DynamoTable(name, options);
    this.hooks = options.hooks || {};

    // Delegate externally accessed properties
    Object.keys(this._delegate).forEach((key) => {
      this[key] = this._delegate[key];
    });

    // NOTE: Rebind all methods so they can be passed without the class instance
    // Since we only instantiate once per table and maintain the instances,
    // the performance should be a big concern
    Object.getOwnPropertyNames(DynamoAdapter.prototype).forEach((key) => {
      if (typeof this[key] === 'function') this[key] = this[key].bind(this);
    });

    // NOTE: This function needs to be rebound to the delegate for its use case
    this['mapToDb'] = this._delegate.mapToDb.bind(this._delegate);
  }

  // Proxies execution to delegate
  _call(methodName, minLength, ...args) {
    validateCall(methodName, args, minLength);

    const cb = args[args.length - 1];

    let method = this._delegate[methodName];

    // TODO: No support for async 'hook'
    this._runHook(methodName);

    if (!isCallback(cb)) method = promisify(method);

    return method.apply(this._delegate, args);
  }

  _runHook(methodName) {
    const hook = this.hooks[methodName];

    return hook ? hook() : null;
  }

  // Allows additional hook for execution BEFORE function
  addHook(methodName, fn) {
    this.hooks[methodName] = fn;
  }

  // Provides instance copy functionality
  copy(options) {
    const overrides = Object.assign({}, this, options);

    return new DynamoAdapter(this.name, overrides);
  }

  // Accepts minimum 1 parameter
  addNew(...args) {
    return this._call('addNew', 1, ...args);
  }

  // Accepts minimum 0 parameters
  count(...args) {
    return this._call('count', 0, ...args);
  }

  // Accepts minimum 1 parameter
  get(...args) {
    return this._call('get', 1, ...args);
  }

  // Accepts minimum 1 parameter
  put(...args) {
    return this._call('put', 1, ...args);
  }

  // Accept minimum 1 parameter
  delete(...args) {
    return this._call('delete', 1, ...args);
  }

  update(...args) {
    return this._call('update', 1, ...args);
  }

  // Accept minimum 1 parameter
  query(...args) {
    return this._call('query', 1, ...args);
  }

  // Accept minimum 0 parameters
  scan(...args) {
    return this._call('scan', 0, ...args);
  }

  // Accept minimum 1 parameter
  batchGet(...args) {
    return this._call('batchGet', 1, ...args);
  }

  // Accept minimum 1 parameter
  batchWrite(...args) {
    return this._call('batchWrite', 1, ...args);
  }

  // Accept minimum 0 parameters
  createTable(...args) {
    return this._call('createTable', 0, ...args);
  }

  // Accept minimum 0 parameters
  describeTable(...args) {
    return this._call('describeTable', 0, ...args);
  }

  // Accept minimum 0 parameters
  deleteTable(...args) {
    return this._call('deleteTable', 0, ...args);
  }

  // Accept minimum 2 parameters
  throttledBatchWrite(...args) {
    return this._call('throttledBatchWrite', 2, ...args);
  }

  // Accept minimum 0 parameters
  truncate(...args) {
    return this._call('truncate', 0, ...args);
  }
}

module.exports = DynamoAdapter;
