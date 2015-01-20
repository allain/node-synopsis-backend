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

module.exports = function(server) {
  shoe(function (stream) {
		stream.on('error', function(err) {
			debug('stream error', err);
		});

    stream.pipe(JSONStream.parse()).pipe(bootstrap(function(config, encoding, cb) {
		  buildSynopsis(config.name, function(err, syn) {
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
		})).pipe(JSONStream.stringify(false)).pipe(stream);
  }).install(server, '/sync');
}

function buildSynopsis(targetName, cb) {
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
      store: buildMongoStore(targetName)
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

  return {
    get: function(key, cb) {
      collection.findOne({key: key}, function(err, doc) {
        if (err) return cb(err);
        cb(null, doc ? doc.val : null);
      });
    },
    set: function(key, val, cb) {
      collection.update({key: key}, {$set: {key: key, val: val}}, {upsert: true, multi: false}, cb);
    }
  };
}
