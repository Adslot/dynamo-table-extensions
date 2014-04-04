dynamo-table-extensions
=======================

[![Build Status](https://secure.travis-ci.org/xarvh/dynamo-table-extensions.png?branch=master)](http://travis-ci.org/xarvh/dynamo-table-extensions)

Adds higher-level methods to [dynamo-table](https://github.com/mhart/dynamo-table).


Extended API
------------

### throttledBatchWrite(capacityRatio, items, callback)

Batch writes `items` ensuring that at most a fraction of the table's write capacity corresponding to `capacityRatio` is used.


Thanks
------

Thanks to [@mhart](https://github.com/mhart) for [dynamo-table](https://github.com/mhart/dynamo-table) upon which this extension is based.

