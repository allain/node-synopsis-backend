var mongojs = require('mongojs');
var bootstrap = require('stream-bootstrap');
var stream = require('stream');

var db = mongojs(process.env.MONGOLAB_URI || 'localhost/sync-test');

var Synopsis = require('synopsis');
var debug = require('debug')('synopsis-store');

var through2 = require('through2');

var jiff = require('jiff');
var JSONStream = require('JSONStream');

var duplexify = require('duplexify');

module.exports = SynopsisBackend;

function SynopsisBackend(options) {
  options = options || {};
  this.targets = {};
  this.createStream = function() {
    var self = this;
    var input = new stream.PassThrough();
    var output = new stream.PassThrough();
    var consumerId;
    var store;

    input.pipe(JSONStream.parse()).pipe(bootstrap(function(config, encoding, cb) {
      consumerId = config.consumerId;
      if (consumerId) {
        debug('consumer connected ' + consumerId);
      } else {
        debug('ERROR: consumerId not found in first payload');
      }

      store = buildMongoStore(config.name);

      checkAuthentication(config.auth, function(err) {
        if (err) {
          var errorStream = new stream.Readable({
            objectMode: true
          });
          errorStream._read = function() {};
          errorStream.push({
            error: 'invalid auth',
            cause: err.toString()
          });
          return cb(null, errorStream);
        }

        if (config.auth) {
          debug('Authed ' + config.auth.network + '-' + config.auth.profile);
        }

        wireUpSynopsysStream(cb);
      });

      function checkAuthentication(auth, cb) {
        if (config.name.match(/^p-/) && !auth) return cb(new Error('Auth not given for personal store'));

        if (auth && options.authenticator) {
          if (typeof(options.authenticator) !== 'function') {
            throw new Error('invalid authenticator');
          }

          debug('calling out to authenticator with auth', auth);

          return options.authenticator(auth, cb);
        }

        return cb(null);
      }

      function wireUpSynopsysStream(cb) {
        buildSynopsis(config, store, self.targets, function(err, syn) {
          if (err) {
            debug('could not create synopsis instance', err);
            return;
          }

          //TODO: handle errors way way better than this
          syn.createStream(config.start, function(err, synStream) {
            cb(null, synStream);
          });
        });
      }

    })).pipe(through2.obj(function(chunk, enc, cb) {
      this.push(chunk);
      if (consumerId) {
        debug('consumer ' + consumerId + ' = ' + chunk[1]);
        store.set('c-' + consumerId, chunk[1]);
      }

      cb();
    })).pipe(JSONStream.stringify(false)).pipe(output);

    output.on('error', function(err) {
      debug('ERROR CALLED', err);
    });

    return duplexify(input, output);
  };
}

function buildSynopsis(config, store, synopsisCache, cb) {
  var targetName = config.name;
  var target = synopsisCache[targetName];
  if (target) {
    debug('reusing model: ' + targetName);
    cb(null, target);
  } else {
    debug('creating model: ' + targetName);

    target = new Synopsis({
      start: {},
      patcher: function(doc, patch) {
        return jiff.patch(patch, doc);
      },
      differ: function(before, after) {
        return jiff.diff(before, after, function(obj) {
          return obj.id || obj._id || obj.hash || JSON.stringify(obj);
        });
      },
      store: store
    });

    target.on('ready', function() {
      debug('target ready', targetName);
      synopsisCache[targetName] = target;
      cb(null, target);
    });
  }
}

function buildMongoStore(name) {
  var collection = db.collection(name);

  function noopErr(err) {
    if (err) {
      debug('error writing to db', err);
    }
  }

  return {
    get: function(key, cb) {
      collection.findOne({
        key: key
      }, function(err, doc) {
        if (err) return cb(err);
        cb(null, doc ? doc.val : null);
      });
    },
    set: function(key, val, cb) {
      collection.update({
        key: key
      }, {
        $set: {
          key: key,
          val: val
        }
      }, {
        upsert: true,
        multi: false
      }, cb || noopErr);
    }
  };
}