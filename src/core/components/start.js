'use strict'

const series = require('async/series')
const Bitswap = require('ipfs-bitswap')
const get = require('lodash/get')
const setImmediate = require('async/setImmediate')
const promisify = require('promisify-es6')
const TieredStore = require('datastore-core').TieredDatastore

const IPNS = require('../ipns')
const DatastorePubsub = require('../ipns/datastore-pubsub')

module.exports = (self) => {
  return promisify((callback) => {
    const done = (err) => {
      if (err) {
        setImmediate(() => self.emit('error', err))
        return callback(err)
      }

      self.state.started()
      setImmediate(() => self.emit('start'))
      callback()
    }

    if (self.state.state() !== 'stopped') {
      return done(new Error(`Not able to start from state: ${self.state.state()}`))
    }

    self.log('starting')
    self.state.start()

    series([
      (cb) => {
        // The repo may be closed if previously stopped
        self._repo.closed
          ? self._repo.open(cb)
          : cb()
      },
      (cb) => self.libp2p.start(cb),
      (cb) => {
        // Setup online routing for IPNS with a tiered routing composed by a DHT and a Pubsub router (if properly enabled)
        const stores = []

        // Add IPNS pubsub if enabled
        if (get(self._options, 'EXPERIMENTAL.ipnsPubsub', false)) {
          // const floodSub = self._libp2pNode._floodSub // TODO should be floodsub?
          const pubsub = self._libp2pNode.pubsub
          const localDatastore = self._repo.datastore
          const peerId = self._peerInfo.id
          const datastorePubsub = new DatastorePubsub(pubsub, localDatastore, peerId)

          stores.push(datastorePubsub)
        }

        // NOTE: For now, the DHT is being replaced by the local repo datastore
        stores.push(self._repo.datastore)

        const routing = new TieredStore(stores)
        self._ipns = new IPNS(routing, self)

        self._bitswap = new Bitswap(
          self._libp2pNode,
          self._repo.blocks,
          { statsEnabled: true }
        )

        self._bitswap.start()
        self._blockService.setExchange(self._bitswap)

        self._preload.start()
        self._ipns.republisher.start()
        self._mfsPreload.start(cb)
      }
    ], done)
  })
}
