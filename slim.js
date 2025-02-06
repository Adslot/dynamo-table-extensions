/* eslint-disable no-shadow */

const async = require('async');
var dynamo;

var once = function (func) {
  var ran = false,
    memo;
  return function () {
    if (ran) return memo;
    ran = true;
    memo = func.apply(this, arguments);
    func = null;
    return memo;
  };
};

module.exports.DynamoTable = DynamoTable;

function DynamoTable(name, options) {
  if (!name) throw new Error('Table must have a name');
  options = options || {};
  this.name = name;
  this.client = options.client;
  if (!this.client) {
    if (!dynamo) throw new Error('dynamo-client module is not installed');
    this.client = dynamo.createClient(options.region, options.credentials);
  }
  this.mappings = options.mappings || {};
  this.key = options.key || Object.keys(options.keyTypes || this.mappings).slice(0, 2);
  if (!Array.isArray(this.key)) this.key = [this.key];
  if (!this.key.length) this.key = ['id'];
  this.keyTypes = options.keyTypes || {};
  this.readCapacity = options.readCapacity;
  this.writeCapacity = options.writeCapacity;
  this.localIndexes = options.localIndexes || options.indexes;
  this.globalIndexes = options.globalIndexes;

  this.postMapFromDb = options.postMapFromDb; // needed
  this.preMapToDb = options.preMapToDb; // needed
}

DynamoTable.prototype.mapAttrToDb = function (val, _key, _jsObj) {
  if (val instanceof Date) return val.toISOString();

  return val;
};

DynamoTable.prototype.mapAttrFromDb = function (val, _key, _dbItem) {
  return val;
};

DynamoTable.prototype.mapToDb = function (jsObj) {
  if (this.preMapToDb) jsObj = this.preMapToDb(jsObj);
  var self = this,
    dbItem = jsObj != null ? {} : null;

  if (dbItem != null && jsObj != null) {
    Object.keys(jsObj).forEach(function (key) {
      var dbAttr = self.mapAttrToDb(jsObj[key], key, jsObj);
      if (!self._isEmpty(dbAttr)) dbItem[key] = dbAttr;
    });
  }

  return dbItem;
};

DynamoTable.prototype.mapFromDb = function (dbItem) {
  // TODO: clone?
  var jsObj = dbItem;

  if (this.postMapFromDb) jsObj = this.postMapFromDb(jsObj);

  return jsObj;
};

DynamoTable.prototype.resolveKey = function (key) {
  var self = this;
  if (arguments.length > 1) key = [].slice.call(arguments);
  else if (typeof key !== 'object' || Buffer.isBuffer(key)) key = [key];
  if (!key) throw new Error('Key is empty: ' + key);

  if (Array.isArray(key)) {
    return key.reduce(function (dbKey, val, ix) {
      var dbAttr = self.mapAttrToDb(val, self.key[ix]);
      if (self._isEmpty(dbAttr)) throw new Error('Key element "' + self.key[ix] + '" is empty: ' + JSON.stringify(val));
      dbKey[self.key[ix]] = dbAttr;
      return dbKey;
    }, {});
  }
  return Object.keys(key).reduce(function (dbKey, attr) {
    var dbAttr = self.mapAttrToDb(key[attr], attr);
    if (self._isEmpty(dbAttr)) throw new Error('Key element "' + attr + '" is empty: ' + JSON.stringify(key[attr]));
    dbKey[attr] = dbAttr;
    return dbKey;
  }, {});
};

DynamoTable.prototype._isEmpty = function (attr) {
  return (
    attr == null ||
    attr.S === '' ||
    attr.N === '' ||
    attr.B === '' ||
    attr.SS === '[]' ||
    attr.NS === '[]' ||
    attr.BS === '[]'
  );
};

DynamoTable.prototype.get = function (key, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options = this._getDefaultOptions(options);
  var self = this;

  options.Key = options.Key || this.resolveKey(key);
  this.client.request('GetItem', options, function (err, data) {
    if (err) return cb(err);

    cb(null, self.mapFromDb(data.Item));
  });
};

DynamoTable.prototype.put = function (jsObj, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options.TableName = options.TableName || this.name;

  options.Item = options.Item || this.mapToDb(jsObj);
  this.client.request('PutItem', options, cb);
};

DynamoTable.prototype.delete = function (key, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options.TableName = options.TableName || this.name;

  options.Key = options.Key || this.resolveKey(key);
  this.client.request('DeleteItem', options, cb);
};

DynamoTable.prototype.update = function (key, actions, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (!cb) {
    cb = actions;
    actions = key;
    key = null;
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options = this._getDefaultOptions(options);
  var self = this,
    pick,
    attrUpdates;

  // If actions is a string or array, then it's a whitelist for attributes to update
  if (typeof actions === 'string') actions = [actions];
  if (Array.isArray(actions)) {
    pick = actions;
    actions = key;
    key = null;
  }

  // If key is null, assume actions has a full object to put so clone it (without keys)
  if (key == null) {
    key = this.key.map(function (attr) {
      return actions[attr];
    });
    pick = pick || Object.keys(actions);
    actions = {
      put: pick.reduce(function (attrsObj, attr) {
        if (!~self.key.indexOf(attr)) attrsObj[attr] = actions[attr];
        return attrsObj;
      }, {}),
    };
  }

  // If we have some attributes that are not actions (put, add, delete), then throw
  if (
    Object.keys(actions).some(function (attr) {
      return !~['put', 'add', 'delete'].indexOf(attr);
    })
  )
    throw new Error('actions must only contain put/add/delete attributes');

  options.Key = options.Key || this.resolveKey(key);
  attrUpdates = options.AttributeUpdates = options.AttributeUpdates || {};

  if (actions.put != null) {
    Object.keys(actions.put).forEach(function (attr) {
      attrUpdates[attr] = attrUpdates[attr] || { Value: self.mapAttrToDb(actions.put[attr], attr) };
      if (self._isEmpty(attrUpdates[attr].Value)) {
        attrUpdates[attr].Action = 'DELETE'; // "empty" attributes should actually be deleted
        delete attrUpdates[attr].Value;
      }
    });
  }

  if (actions.add != null) {
    Object.keys(actions.add).forEach(function (attr) {
      attrUpdates[attr] = attrUpdates[attr] || { Action: 'ADD', Value: self.mapAttrToDb(actions.add[attr], attr) };
    });
  }

  if (actions.delete != null) {
    if (!Array.isArray(actions.delete)) actions.delete = [actions.delete];
    actions.delete.forEach(function (attr) {
      if (typeof attr === 'string') {
        attrUpdates[attr] = attrUpdates[attr] || { Action: 'DELETE' };
      } else {
        Object.keys(attr).forEach(function (setKey) {
          attrUpdates[setKey] = attrUpdates[setKey] || {
            Action: 'DELETE',
            Value: self.mapAttrToDb(attr[setKey], setKey),
          };
        });
      }
    });
  }

  if (!Object.keys(attrUpdates).length) return process.nextTick(cb);

  this.client.request('UpdateItem', options, cb);
};

DynamoTable.prototype.query = function (conditions, options, cb) {
  // console.log('query: ', this.name)
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options = this._getDefaultOptions(options);

  options.KeyConditions = options.KeyConditions || this.conditions(conditions);

  this._listRequest('Query', options, cb);
};

DynamoTable.prototype.scan = function (conditions, options, cb) {
  // console.log('scan: ', this.name)
  if (!cb) {
    cb = options;
    options = {};
  }
  if (!cb) {
    cb = conditions;
    conditions = null;
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  cb = once(cb);
  options = this._getDefaultOptions(options);

  if (conditions != null && !options.ScanFilter) options.ScanFilter = this.conditions(conditions);

  this._listRequest('Scan', options, cb);
};

// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
DynamoTable.MAX_GET = 250;
DynamoTable.prototype.batchGet = function (keys, options, tables, cb) {
  if (!cb) {
    cb = tables;
    tables = [];
  }
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  cb = once(cb);
  var self = this,
    onlyThis = !tables.length,
    tablesByName = {},
    allKeys,
    numRequests,
    allResults,
    i,
    j,
    key,
    requestItems,
    requestItem,
    opt;

  if (Array.isArray(keys)) tables.unshift({ table: this, keys: keys, options: options });

  allKeys = tables.map(function (tableObj) {
    var table = tableObj.table,
      keys = tableObj.keys,
      options = tableObj.options;
    tablesByName[table.name] = table;
    if (Array.isArray(options)) options = { AttributesToGet: options };
    else if (typeof options === 'string') options = { AttributesToGet: [options] };
    return keys.map(function (key) {
      // eslint-disable-next-line array-callback-return
      if (!key) return;
      var dbKey = table.resolveKey(key);
      dbKey._table = table.name;
      dbKey._options = options || {};
      return dbKey;
    });
  });
  allKeys = [].concat.apply([], allKeys).filter(function (key) {
    return key != null;
  });
  numRequests = Math.ceil(allKeys.length / DynamoTable.MAX_GET);
  allResults = new Array(numRequests);

  // TODO: Not sure here... should we throw an error?
  if (!numRequests) {
    if (onlyThis) return cb(null, []);
    return cb(
      null,
      tables.reduce(function (merged, table) {
        merged[table.name] = [];
        return merged;
      }, {})
    );
  }

  for (i = 0; i < allKeys.length; i += DynamoTable.MAX_GET) {
    requestItems = {};
    for (j = i; j < i + DynamoTable.MAX_GET && j < allKeys.length; j++) {
      key = allKeys[j];
      requestItem = requestItems[key._table] = requestItems[key._table] || {};
      for (opt in key._options) requestItem[opt] = key._options[opt];
      requestItem.Keys = requestItem.Keys || [];
      requestItem.Keys.push(key);
      delete key._table;
      delete key._options;
    }
    batchRequest(requestItems, checkDone(i / DynamoTable.MAX_GET));
  }

  function batchRequest(requestItems, results, cb) {
    if (!cb) {
      cb = results;
      results = {};
    }
    self.client.request('BatchGetItem', { RequestItems: requestItems }, function (err, data) {
      if (err) return cb(err);
      for (var name in data.Responses) {
        results[name] = (results[name] || []).concat(
          data.Responses[name].map(tablesByName[name].mapFromDb.bind(tablesByName[name]))
        );
      }
      if (Object.keys(data.UnprocessedKeys || {}).length) return batchRequest(data.UnprocessedKeys, results, cb);
      cb(null, results);
    });
  }

  function checkDone(ix) {
    return function (err, results) {
      if (err) return cb(err);
      allResults[ix] = results;
      if (!--numRequests) {
        var merged = {};
        allResults.forEach(function (results) {
          for (var name in results) merged[name] = (merged[name] || []).concat(results[name]);
        });
        cb(null, onlyThis ? merged[self.name] : merged);
      }
    };
  }
};

// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
DynamoTable.MAX_WRITE = 25;
DynamoTable.prototype.batchWrite = function (operations, tables, cb) {
  if (!cb) {
    cb = tables;
    tables = [];
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  cb = once(cb);
  var self = this,
    allOperations,
    numRequests,
    i,
    j,
    requestItems,
    operation;

  if (operations && Object.keys(operations).length) tables.unshift({ table: this, operations: operations });

  allOperations = tables.map(function (tableObj) {
    var table = tableObj.table,
      operations = tableObj.operations || [],
      ops;
    if (Array.isArray(operations)) operations = { puts: operations, deletes: [] };
    ops = (operations.puts || []).map(function (jsObj) {
      return { PutRequest: { Item: table.mapToDb(jsObj) }, _table: table.name };
    });
    return ops.concat(
      (operations.deletes || []).map(function (key) {
        return { DeleteRequest: { Key: table.resolveKey(key) }, _table: table.name };
      })
    );
  });
  allOperations = [].concat.apply([], allOperations);
  numRequests = Math.ceil(allOperations.length / DynamoTable.MAX_WRITE);

  // TODO: Not sure here... should we throw an error?
  if (!numRequests) return cb();

  for (i = 0; i < allOperations.length; i += DynamoTable.MAX_WRITE) {
    requestItems = {};
    for (j = i; j < i + DynamoTable.MAX_WRITE && j < allOperations.length; j++) {
      operation = allOperations[j];
      requestItems[operation._table] = requestItems[operation._table] || [];
      requestItems[operation._table].push(operation);
      delete operation._table;
    }
    batchRequest(requestItems, checkDone);
  }

  function batchRequest(requestItems, cb) {
    self.client.request('BatchWriteItem', { RequestItems: requestItems }, function (err, data) {
      if (err) return cb(err);
      if (Object.keys(data.UnprocessedItems || {}).length) return batchRequest(data.UnprocessedItems, cb);
      cb();
    });
  }

  function checkDone(err) {
    if (err) return cb(err);
    if (!--numRequests) cb();
  }
};

DynamoTable.prototype.createTable = function (readCapacity, writeCapacity, localIndexes, options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (!cb) {
    cb = localIndexes;
    localIndexes = this.localIndexes;
  }
  if (!cb) {
    cb = writeCapacity;
    writeCapacity = this.writeCapacity || 1;
  }
  if (!cb) {
    cb = readCapacity;
    readCapacity = this.readCapacity || 1;
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options.TableName = options.TableName || this.name;

  this.client.request('CreateTable', options, function (err, data) {
    if (err) return cb(err);
    cb(null, data.TableDescription);
  });
};

DynamoTable.prototype.describeTable = function (options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options.TableName = options.TableName || this.name;

  this.client.request('DescribeTable', options, function (err, data) {
    if (err) return cb(err);
    cb(null, data.Table);
  });
};

DynamoTable.prototype.deleteTable = function (options, cb) {
  if (!cb) {
    cb = options;
    options = {};
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  options.TableName = options.TableName || this.name;

  this.client.request('DeleteTable', options, function (err, data) {
    if (err) return cb(err);
    cb(null, data.TableDescription);
  });
};

DynamoTable.prototype._listRequest = function (operation, items, options, cb) {
  if (!cb) {
    cb = options;
    options = items;
    items = [];
  }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function');
  var self = this;

  this.client.request(operation, options, function (err, data) {
    if (err) return cb(err);
    if (options.Select === 'COUNT') return cb(null, data.Count);

    // When we fetch only few fields it doesn't require
    // mapping as it's not longer a valid model, right?
    if (options.AttributesToGet && options.AttributesToGet.length) return cb(null, data.Items);

    for (var i = data.Items.length - 1; i >= 0; i--) {
      data.Items[i] = self.mapFromDb(data.Items[i]);
    }

    cb(null, data.Items);
  });
};

DynamoTable.prototype.conditions = function (conditionExprObj) {
  var condObj = {};
  for (var k in conditionExprObj) condObj[k] = this.condition(k, conditionExprObj[k]);
  return condObj;
};

DynamoTable.prototype.condition = function (key, conditionExpr) {
  var self = this,
    type = typeof conditionExpr,
    comparison,
    attrVals,
    cond,
    hasEmpty = false;

  if (conditionExpr == null) {
    comparison = 'NULL';
  } else if (conditionExpr === 'notNull' || conditionExpr === 'NOT_NULL') {
    comparison = 'NOT_NULL';
  } else if (type === 'string' || type === 'number' || type === 'boolean' || Buffer.isBuffer(conditionExpr)) {
    comparison = 'EQ';
    attrVals = [conditionExpr];
  } else if (Array.isArray(conditionExpr)) {
    comparison = 'IN';
    attrVals = conditionExpr;
  } else {
    comparison = Object.keys(conditionExpr)[0];
    attrVals = conditionExpr[comparison];
    if (!Array.isArray(attrVals)) attrVals = [attrVals];
    comparison = this.comparison(comparison);
  }
  cond = { ComparisonOperator: comparison };
  if (attrVals != null) {
    cond.AttributeValueList = attrVals.map(function (val) {
      var dbVal = self.mapAttrToDb(val, key);
      if (self._isEmpty(dbVal)) hasEmpty = true;
      return dbVal;
    });
    // Special case for when we have empty strings or nulls in our conditions
    if (hasEmpty) {
      delete cond.AttributeValueList;
      cond.ComparisonOperator = cond.ComparisonOperator === 'EQ' ? 'NULL' : 'NOT_NULL';
    }
  }
  return cond;
};

DynamoTable.prototype.comparison = function (comparison) {
  // eslint-disable-next-line default-case
  switch (comparison) {
    case '=':
      return 'EQ';
    case '==':
      return 'EQ';
    case '!=':
      return 'NE';
    case '<=':
      return 'LE';
    case '<':
      return 'LT';
    case '>':
      return 'GT';
    case '>=':
      return 'GE';
    case '>=<=':
      return 'BETWEEN';
    case 'beginsWith':
    case 'startsWith':
      return 'BEGINS_WITH';
    case 'notContains':
    case 'doesNotContain':
      return 'NOT_CONTAINS';
  }
  return comparison.toUpperCase();
};

DynamoTable.prototype._getDefaultOptions = function (options) {
  if (options == null) options = {};
  else if (Array.isArray(options)) options = { AttributesToGet: options };
  else if (typeof options === 'string') options = { AttributesToGet: [options] };
  options.TableName = options.TableName || this.name;
  return options;
};

// Ensures that no more than capacityRatio * writeCapacity items are written per second
DynamoTable.prototype.throttledBatchWrite = function (capacityRatio, items, cb) {
  if (!(capacityRatio > 0)) return cb(new Error('non-positive capacityRatio'));

  var self = this;
  self.describeTable(function (err, info) {
    if (err) return cb(err);

    var itemsPerSecond = Math.ceil(info.ProvisionedThroughput.WriteCapacityUnits * capacityRatio);
    var written = 0;
    var ready = true;

    var waitAndWrite = function (cb) {
      async.until(
        function () {
          return ready;
        },
        function (cb) {
          setTimeout(cb, 10);
        },
        function (err) {
          if (err) return cb(err);
          ready = false;
          setTimeout(function () {
            ready = true;
          }, 1000);

          var write = items.slice(written, written + itemsPerSecond);
          self.batchWrite(write, function (err) {
            if (err) return cb(err);
            written += write.length;
            cb();
          });
        }
      );
    };

    async.whilst(
      function () {
        return written < items.length;
      },
      waitAndWrite,
      cb
    );
  });
};

DynamoTable.prototype.truncate = function (cb) {
  async.series([this.deleteTable.bind(this), this.createTable.bind(this)], cb);
};

DynamoTable.prototype.addNew = function (record, cb) {
  var key = Array.isArray(this.key) ? this.key[0] : this.key;

  if (record[key]) return this.put(record, cb);

  var self = this;

  // this.nextId is added by `dynamo-table-id`, which must be loaded
  this.nextId(function (err, id) {
    if (err) return cb(err);
    record[key] = id;
    self.put(record, cb);
  });
};
