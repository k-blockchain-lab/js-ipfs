'use strict'

const ipns = require('ipns')
const PeerId = require('peer-id')
const Record = require('libp2p-record').Record
const { Key } = require('interface-datastore')
const errcode = require('err-code')

const debug = require('debug')
const log = debug('jsipfs:ipns:publisher')
log.error = debug('jsipfs:ipns:publisher:error')

// DatastorePubsub is responsible for providing an interface-datastore compliant api
class DatastorePubsub {
  constructor (pubsub, datastore, peerId) {
    this._pubsub = pubsub
    this._datastore = datastore
    this._peerId = peerId

    this._handleSubscription = this._handleSubscription.bind(this)
  }

  /**
   * Publishes an IPNS record through pubsub.
   * @param {Key} key ipfs path of the object to be published.
   * @param {Buffer} val ipns entry data structure.
   * @param {function(Error)} callback
   * @returns {void}
   */
  put (key, val, callback) {
    if (!(key instanceof Key)) {
      const errMsg = `datastore key does not have a valid format`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_DATASTORE_KEY'))
    }

    if (!Buffer.isBuffer(val)) {
      const errMsg = `received ipns entry that is not a buffer`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_RECORD_RECEIVED'))
    }

    log(`publish value for key ${key}`)

    // Publish record to pubsub
    const stringifiedTopic = key._buf.toString()
    this._pubsub.publish(stringifiedTopic, val, callback)
  }

  /**
   * Try to subscribe an IPNS record with Pubsub and returns the local value if available.
   * @param {Key} key path of the object to be queried.
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   */
  get (key, callback) {
    if (!(key instanceof Key)) {
      const errMsg = `datastore key does not have a valid format`

      log.error(errMsg)
      return callback(errcode(new Error(errMsg), 'ERR_INVALID_DATASTORE_KEY'))
    }

    log(`subscribe values for key ${key}`)

    // Subscribe
    const stringifiedTopic = key._buf.toString()
    this._pubsub.subscribe(stringifiedTopic, this._handleSubscription, (err) => {
      if (err) {
        const errMsg = `cannot subscribe topic ${stringifiedTopic}`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_SUBSCRIBING_TOPIC'))
      }

      this._getLocal(key, callback)
    })
  }

  _getLocal (key, callback) {
    // Get from local datastore
    this._datastore.get(key, (err, dsVal) => {
      if (err) {
        const errMsg = `local record requested was not found for ${key}`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_NO_LOCAL_RECORD_FOUND'))
      }

      if (!Buffer.isBuffer(dsVal)) {
        const errMsg = `found ipns record that we couldn't convert to a value`

        log.error(errMsg)
        return callback(errcode(new Error(errMsg), 'ERR_INVALID_RECORD_RECEIVED'))
      }

      const record = Record.deserialize(dsVal)
      const ipnsEntry = ipns.unmarshal(record.value)

      this._validateRecord(this._peerId, ipnsEntry, (err) => {
        if (err) {
          return callback(err)
        }
        callback(null, dsVal)
      })
    })
  }

  _isMoreRecent (key, val, callback) {
    const receivedRecord = Record.deserialize(val)
    const receivedIpnsEntry = ipns.unmarshal(receivedRecord.value)

    const b58peerId = receivedRecord.author.toB58String()
    const peerId = PeerId.createFromB58String(b58peerId)

    // Validate received record
    this._validateRecord(peerId, receivedIpnsEntry, (err) => {
      if (err) {
        return callback(err)
      }

      // Get Local Key
      const ipnsKey = new Key(key)
      this._getLocal(ipnsKey, (err, res) => {
        // if the old one is invalid, the new one is *always* better
        if (err) {
          return callback(null, true)
        }

        // if the same record, do not need to store
        if (res.equals(val)) {
          return callback(null, false)
        }

        // select the best according to the sequence number
        const localRecord = Record.deserialize(res)
        const localIpnsEntry = ipns.unmarshal(localRecord.value)

        return callback(null, receivedIpnsEntry.sequence > localIpnsEntry.sequence)
      })
    })
  }

  _handleSubscription (msg) {
    const { data, from, topicIDs } = msg
    const key = topicIDs[0]

    // Stop if the message is from the peer (it already stored it)
    if (from === this._peerId.toB58String()) {
      return
    }

    // Verify if the record is more recent than the current one being stored
    this._isMoreRecent(key, data, (err, isMoreRecent) => {
      if (err) {
        return
      }

      if (isMoreRecent) {
        // Add record to routing
        this._datastore.put(key, data, (err) => {
          if (err) {
            log.error(`ipns record for ${key.toString()} could not be stored in the routing`)
            return
          }

          log(`ipns record for ${key.toString()} was stored in the datastore`)
        })
      }
    })
  }

  _validateRecord (peerId, entry, callback) {
    // extract public key
    ipns.extractPublicKey(peerId, entry, (err, pubKey) => {
      if (err) {
        return callback(err)
      }

      // Record validation
      ipns.validate(pubKey, entry, (err) => {
        if (err) {
          return callback(err)
        }

        callback(null, entry)
      })
    })
  }
}

exports = module.exports = DatastorePubsub
