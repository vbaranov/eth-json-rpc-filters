const Mutex = require('await-semaphore').Mutex
const EthQuery = require('ethjs-query')
const createAsyncMiddleware = require('json-rpc-engine/src/createAsyncMiddleware')
const createJsonRpcMiddleware = require('eth-json-rpc-middleware/scaffold')
const LogFilter = require('./log-filter.js')
const BlockFilter = require('./block-filter.js')
const TxFilter = require('./tx-filter.js')
const { intToHex, hexToInt } = require('./hexUtils')

module.exports = createEthFilterMiddleware

function createEthFilterMiddleware({ blockTracker, provider }) {

  // ethQuery for log lookups
  const ethQuery = new EthQuery(provider)
  // create filter collection
  let filterIndex = 0
  const filters = {}
  // create update mutex
  const mutex = new Mutex()
  const waitForFree = mutexMiddlewareWrapper({ mutex })

  const middleware = createJsonRpcMiddleware({
    // install filters
    eth_newFilter:                   waitForFree(createAsyncMiddleware(newLogFilter)),
    eth_newBlockFilter:              waitForFree(createAsyncMiddleware(newBlockFilter)),
    eth_newPendingTransactionFilter: waitForFree(createAsyncMiddleware(newPendingTransactionFilter)),
    // uninstall filters
    eth_uninstallFilter:             waitForFree(createAsyncMiddleware(uninstallFilter)),
    // checking filter changes
    eth_getFilterChanges:            waitForFree(createAsyncMiddleware(getFilterChanges)),
    eth_getFilterLogs:               waitForFree(createAsyncMiddleware(getFilterLogs)),
  })

  // setup filter updating and destroy handler
  const filterUpdater = async ({ oldBlock, newBlock }) => {
    if (filters.length === 0) return
    // lock update reads
    const releaseLock = await mutex.acquire()
    // process all filters in parallel
    await Promise.all(objValues(filters).map((filter) => {
      return filter.update({ oldBlock, newBlock })
    }))
    // unlock update reads
    releaseLock()
  }
  blockTracker.on('sync', filterUpdater)
  middleware.destroy = () => {
    blockTracker.removeListener('sync', filterUpdater)
  }

  return middleware

  //
  // new filters
  //

  async function newLogFilter(req, res, next) {
    const params = req.params[0]
    const filter = new LogFilter({ ethQuery, params })
    const filterIndex = await installFilter(filter)
    const result = intToHex(filterIndex)
    res.result = result
  }

  async function newBlockFilter(req, res, next) {
    const filter = new BlockFilter({ ethQuery })
    const filterIndex = await installFilter(filter)
    const result = intToHex(filterIndex)
    res.result = result
  }

  async function newPendingTransactionFilter(req, res, next) {
    const filter = new TxFilter({ ethQuery })
    const filterIndex = await installFilter(filter)
    const result = intToHex(filterIndex)
    res.result = result
  }

  //
  // get filter changes
  //

  async function getFilterChanges(req, res, next) {
    const filterIndexHex = req.params[0]
    const filterIndex = hexToInt(filterIndexHex)
    const filter = filters[filterIndex]
    if (!filter) {
      throw new Error('No filter for index "${filterIndex}"')
    }
    const results = filter.getChangesAndClear()
    res.result = results
  }

  async function getFilterLogs(req, res, next, end) {
    const filterIndexHex = req.params[0]
    const filterIndex = hexToInt(filterIndexHex)
    const filter = filters[filterIndex]
    if (!filter) {
      throw new Error('No filter for index "${filterIndex}"')
    }
    const results = filter.getAllResults()
    res.result = results
  }


  //
  // remove filters
  //


  async function uninstallFilter(req, res, next) {
    const filterIndexHex = req.params[0]
    const filterIndex = hexToInt(filterIndexHex)
    const filter = filters[filterIndex]
    const results = Boolean(filter)
    delete filters[filterIndex]
    res.result = results
  }

  async function uninstallAllFilters() {
    const prevFilterCount = objValues(filters).length
    filters = {}
    // update block tracker subs
    updateBlockTrackerSubs({ prevFilterCount, newFilterCount: 0 })
  }

  function updateBlockTrackerSubs({ prevFilterCount, newFilterCount }) {
    // subscribe
    if (prevFilterCount === 0 && newFilterCount > 0) {
      blockTracker.on('sync', filterUpdater)
      return
    }
    // unsubscribe
    if (prevFilterCount > 0 && newFilterCount === 0) {
      blockTracker.removeListener('sync', filterUpdater)
      return
    }
  }

  //
  // utils
  //

  async function installFilter(filter) {
    const currentBlock = await blockTracker.getLatestBlock()
    await filter.initialize({ currentBlock })
    filterIndex++
    filters[filterIndex] = filter
    return filterIndex
  }

}

// helper for turning filter constructors into rpc middleware
function toFilterCreationMiddleware(createFilterFn) {
  return toAsyncRpcMiddleware(async (...args) => {
    const filter = await createFilterFn(...args)
    const result = intToHex(filter.id)
    return result
  })
}

// helper for pulling out req.params and setting res.result
function toAsyncRpcMiddleware(asyncFn) {
  return createAsyncMiddleware(async (req, res) => {
    const result = await asyncFn.apply(null, req.params)
    res.result = result
  })
}

function mutexMiddlewareWrapper({ mutex }) {
  return (middleware) => {
    return async (req, res, next, end) => {
      // wait for mutex available
      // we can release immediately because
      // we just need to make sure updates aren't active
      const releaseLock = await mutex.acquire()
      releaseLock()
      middleware(req, res, next, end)
    }
  }
}

function objValues(obj, fn){
  const values = []
  for (let key in obj) {
    values.push(obj[key])
  }
  return values
}
