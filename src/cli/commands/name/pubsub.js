'use strict'

/*
Manage and inspect the state of the IPNS pubsub resolver.
Note: this command is experimental and subject to change as the system is refined.
*/
module.exports = {
  command: 'pubsub',

  description: 'IPNS pubsub management.',

  builder (yargs) {
    return yargs.commandDir('pubsub')
  },

  handler (argv) {
  }
}

// TODO: DAEMON SHOULD START WITH --enable-namesys-pubsub
