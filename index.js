const { DynamoTable } = require('./slim');
const DynamoAdapter = require('./dynamo-adapter');

// Library is a factory function which also provides the DynamoTable class
const dynamo = (...args) => new DynamoAdapter(...args);

dynamo.DynamoTable = DynamoTable;

module.exports = dynamo;
