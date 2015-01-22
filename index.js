var shoe = require('shoe');
var mongojs = require('mongojs');
var bootstrap = require('stream-bootstrap');

var db = mongojs(process.env.MONGOLAB_URI || 'localhost/sync-test');

var Synopsis = require('synopsis');
var debug = require('debug')('synopsis-store');

var through2 = require('through2');

var jiff = require('jiff');
var JSONStream = require('JSONStream');

var targets = {};

module.exports = function(server, options) {
  options = options || {};

  shoe(function (stream) {
		stream.on('error', function(err) {
			debug('stream error', err);
		});

		var consumerId;
    var store;

    stream.pipe(JSONStream.parse()).pipe(bootstrap(function(config, encoding, cb) {
      consumerId = config.consumerId;
      if (!consumerId) {
				return cb(new Error('consumerId not found in first payload'));
			}
      debug('consumer connected ' + consumerId);

		  store = buildMongoStore(config.name);

			var auth = config.auth;;
      
			checkAuthentication(auth, wireUpSynopsysStream); 
		  
			function wireUpSynopsysStream(err) {
        if (err) return cb(err);

				buildSynopsis(config, store, function(err, syn) {
					if (err) {
						debug('could not create synopsis', err);
						stream.close();
						return;
					}

					//TODO: handle errors way way better than this
					syn.createStream(config.start, function(err, synStream) {
						cb(null, synStream);
					});
				});	
			}

			function checkAuthentication(auth, cb) {
				if (config.authenticator) {
					if (typeof(auth) !== 'function') {
						throw new Error('invalid authenticator');
					}

					debug('calling out to authenticator with auth', auth);
					return config.authenticator(auth, cb);
				}

				//Placeholder for custom logic
				return cb(null);
			}
		})).pipe(through2.obj(function(chunk, enc, cb) {
			this.push(chunk);
			debug('consumer ' + consumerId + ' = ' + chunk[1]);
			store.set('c-' + consumerId, chunk[1]);
      cb();
		})).pipe(JSONStream.stringify(false)).pipe(stream);
  }).install(server, '/sync');

}


function buildSynopsis(config, store, cb) {
  var targetName = config.name;
  var target = targets[targetName];
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
      targets[targetName] = target;
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
      collection.findOne({key: key}, function(err, doc) {
        if (err) return cb(err);
        cb(null, doc ? doc.val : null);
      });
    },
    set: function(key, val, cb) {
      collection.update({key: key}, {$set: {key: key, val: val}}, {upsert: true, multi: false}, cb || noopErr);
    }
  };
}

