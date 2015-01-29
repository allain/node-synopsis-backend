var mongojs = require('mongojs');

var bootstrap = require('stream-bootstrap');
var stream = require('stream');

var db = mongojs(process.env.MONGOLAB_URI || 'localhost/sync-test');

var Synopsis = require('synopsis');
var debug = require('debug')('synopsis-store');

var through2 = require('through2');

var jiff = require('jiff');
var uuid = require('uuid');
var JSONStream = require('JSONStream');

var duplexify = require('duplexify');

module.exports = SynopsisBackend;

var sessions = db.collection('sessions');

function SynopsisBackend(options) {
  options = options || {};
  this.targets = {};

  function getSession(sid, cb) {
    sessions.findOne({
      sid: sid
    }, function(err, doc) {
      if (err) return cb(err);

      cb(null, doc.content);
    });
  }

  function setSession(sid, content, cb) {
    sessions.update({
      sid: sid
    }, {
      $set: {
        sid: sid,
        content: content
      }
    }, {
      upsert: true,
      multi: false
    }, cb);
  }

  this.createStream = function() {
    var self = this;
    var input = new stream.PassThrough();
    var output = new stream.PassThrough();
    var consumerId;
    var store;

    input.pipe(JSONStream.parse()).pipe(bootstrap(function(config, encoding, cb) {
      consumerId = config.consumerId;
      if (consumerId) {
        debug('consumer connected ' + consumerId + ' to ' + config.name);
      } else {
        debug('ERROR: consumerId not found in first payload');
      }

      store = buildMongoStore(config.name);

      if (config.sid) {
        return getSession(config.sid, function(err, session) {
          if (err) {
            return failBootstrap({
              error: 'unable to fetch session',
              cause: err.toString()
            }, cb);
          } else if (!session) {
            return failBootstrap({
              error: 'unable to find session',
              cause: err.toString()
            }, cb);
          }

          debug('session ' + config.sid + ' => ' + JSON.stringify(session));

          wireUpSynopsysStream(undefined, cb);
        });
      }

      checkAuthentication(config.auth, function(err) {
        if (err) {
          return failBootstrap({
            error: 'invalid auth',
            cause: err.toString()
          }, cb);
        }

        var sessionId;
        if (config.auth && typeof config.auth !== 'string') {
          sessionId = uuid.v4();
          setSession(sessionId, config.auth, function(err) {
            if (err) {
              debug('unable to store session', err);
            }
          });
        }

        if (config.auth) {
          debug('Authed ' + config.auth.network + '-' + config.auth.profile);
        }

        wireUpSynopsysStream(sessionId, cb);
      });

      function failBootstrap(explanation, cb) {
        var errorStream = new stream.Readable({
          objectMode: true
        });
        errorStream._read = function() {};
        errorStream.push(explanation);
        return cb(null, errorStream);
      }

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

      function wireUpSynopsysStream(sessionId, cb) {
        buildSynopsis(config, store, self.targets, function(err, syn) {
          if (err) {
            debug('could not create synopsis instance', err);
            return;
          }

          //TODO: handle errors way way better than this
          syn.createStream(config.start, function(err, synStream) {
            if (err) {
              return failBootstrap({
                error: 'error creating synopsis stream',
                cause: err.toString()
              }, cb);
            }

            if (sessionId) {
              synStream.push({
                sid: sessionId
              });
            }

            cb(null, synStream);
          });
        });
      }

    })).pipe(through2.obj(function(chunk, enc, cb) {
      this.push(chunk);
      if (consumerId && chunk[1] !== void 0) {
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
      patcher: function(doc, patch, cb) {
        try {
          cb(null, jiff.patch(patch, doc));
        } catch (e) {
          cb(e);
        }
      },
      differ: function(before, after, cb) {
        var diff = jiff.diff(before, after, function(obj) {
          return obj.id || obj._id || obj.hash || JSON.stringify(obj);
        });

        cb(null, diff);
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
