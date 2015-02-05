"use strict"

var defaults = require('defaults');
var async = require('async');
var bootstrap = require('stream-bootstrap');
var stream = require('stream');

var Synopsis = require('synopsis');
var debug = require('debug')('synopsis-store');

var jiff = require('jiff');
var uuid = require('uuid');
var JSONStream = require('JSONStream');

var duplexify = require('duplexify');
var Emitter = require('wildemitter');

module.exports = SynopsisBackend;

function SynopsisBackend(options) {
  Emitter.call(this);
  var self = this;

  this.targets = {};
  this.options = options = defaults(options, {
		makeStore: function(name, cb) {
			// Memory Store Maker
			var values = {};

			cb(null, {
				get: function(key, cb) {
					return cb(null, values[key]);
				},
				set: function(key, val, cb) {
					values[key] = val;
          cb();
				}
			});
		}
	});

  

  if (options.sessionStore) {
		process.nextTick(function() {
			self.emit('ready');
		});
	} else {
		options.makeStore('-session', function(err, store) {
			if (err) return self.emit('build-error');

			options.sessionStore = store;
			process.nextTick(function() {
				self.emit('ready');	
			});
		});
	} 
}

SynopsisBackend.prototype = new Emitter();

SynopsisBackend.prototype.createStream = function() {
	var self = this;
	var options = this.options;
  var sessionStore = options.sessionStore;
	
	var input = new stream.PassThrough();
	var output = new stream.PassThrough();

	input.pipe(JSONStream.parse()).pipe(bootstrap(function(handshake, encoding, cb) {
    var store; 
    var sessionId;
    var synopsis;
 
    async.series([
			validateSession,
      checkAuthentication,
		  createSessionIfNeeded,
			buildStore,
      buildSynopsis
		], function(errorStream) {
			// Instead of handling the error normally, let's send back an error to the client inline in the stream
      if (errorStream) return cb(null, errorStream);
			
			synopsis.createStream(handshake.start, function(err, synopsisStream) {
				if (err) 
					return failBootstrep('unable to create synopsis stream', err, cb);
		
    		if (sessionId) {
					synopsisStream.push({
						sid: sessionId
					});
				}

				cb(null, synopsisStream);
			});
		});

    function validateSession(cb) {
			if (!handshake.sid) return cb(); // no need to validate, you don't have one

			return sessionStore.get(handshake.sid, function(err, session) {
				if (err) return failBootstrap('unable to fetch session', err, cb);
				if (!session) return failBootstrap('session not found', cb);

				debug('session ' + handshake.sid + ' => ' + JSON.stringify(session));

        handshake.auth = session;
				cb();
			});
		}
		
    function checkAuthentication(cb) {
			if (handshake.name.match(/^p-/) && !handshake.auth)
				return failBootstrap('auth not given for personal store', cb);

			if (!handshake.auth || !options.authenticator) 
				return cb(); // no need to check auth
				
      if (typeof(options.authenticator) !== 'function')
				return failBootstrap('invalid authenticator', cb);

			debug('calling out to authenticator with auth', handshake.auth);

			return options.authenticator(handshake.auth, function(err) {
			  if (err) return failBootstrap('invalid auth', cb);
        cb();
			});
		}

		function createSessionIfNeeded(cb) {
			if (!handshake.auth || typeof handshake.auth === 'string') 
				return cb(); // if auth is a string, its a session id
			
			sessionId = uuid.v4();	
			sessionStore.set(sessionId, handshake.auth, cb);
		}

		function buildStore(cb) {
			options.makeStore(handshake.name, function(err, s) {
				if (err) return failBootstrap('unable to make store', err, cb);

				store = s;
        cb(null, s);
			});
		}

		function buildSynopsis(cb) {
			var targetName = handshake.name;
			synopsis = self.targets[targetName];
			if (synopsis) {
				debug('reusing model: ' + targetName);
				return cb(null, synopsis);
			}
 
			debug('creating synopis: ' + targetName);

			synopsis = new Synopsis({
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

			synopsis.on('ready', function() {
				debug('synopsis ready', targetName);
				self.targets[targetName] = synopsis;
				cb(null, synopsis);
			});
		}
	})).pipe(JSONStream.stringify(false)).pipe(output);

	function failBootstrap(explanation, rootError, cb) {
		var errorStream = new stream.Readable({
			objectMode: true
		});

		errorStream._read = function() {};

    var errObject = {
			error: explanation,
			cause: cb && rootError ? rootError.toString() : undefined
		};			
		errorStream.push(errObject);

		cb = cb || rootError;    

		return cb(errorStream);
	} 

	output.on('error', function(err) {
		debug('ERROR CALLED', err);
	});

	return duplexify(input, output);
};

