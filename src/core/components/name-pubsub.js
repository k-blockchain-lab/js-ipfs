'use strict'

const debug = require('debug')
const errcode = require('err-code')
const promisify = require('promisify-es6')

const log = debug('jsipfs:namePubsub')
log.error = debug('jsipfs:namePubsub:error')

const isNamePubsubEnabled = (ipfsCore) => (
  ipfsCore._options.EXPERIMENTAL.ipnsPubsub && ipfsCore._libp2pNode._floodSub
)

module.exports = function namePubsub (self) {
  return {
    /**
     * Query the state of IPNS pubsub.
     *
     * @returns {Promise|void}
     */
    state: promisify((callback) => {
      callback(null, {
        enabled: Boolean(isNamePubsubEnabled(self))
      })
    }),
    /**
     * Cancel a name subscription.
     *
     * @param {String} name subscription name.
     * @param {function(Error)} [callback]
     * @returns {Promise|void}
     */
    cancel: promisify((name, callback) => {
      if (!isNamePubsubEnabled(self)) {
        const errMsg = 'IPNS pubsub subsystem is not enabled'

        log.error(errMsg)
        return callback(errcode(errMsg, 'ERR_IPNS_PS_NOT_ENABLED'))
      }

      // Trim /ipns/ prefix from the name

      // Verify peerId

      // Cancel pubsub subscription

      callback(null, {
        canceled: true
      })
    }),
    /**
     * Show current name subscriptions.
     *
     * @param {function(Error)} [callback]
     * @returns {Promise|void}
     */
    subs: promisify((callback) => {
      if (!isNamePubsubEnabled(self)) {
        const errMsg = 'IPNS pubsub subsystem is not enabled'

        log.error(errMsg)
        return callback(errcode(errMsg, 'ERR_IPNS_PS_NOT_ENABLED'))
      }

      // Get pubsub subscriptions

      // Iterate over subscriptions

      // Split record key

      // Try to create peerId from a string
      // If error: `ipns key is not a valid peer ID`
      // If not error: append to the list

      callback(null, {
        strings: 'subs'
      })
    })
  }
}
