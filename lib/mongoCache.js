var mongo = require('mongodb');
var MongoClient = require('mongodb').MongoClient;

var mongoUri = process.env.MONGOLAB_URI ||
    process.env.MONGOHQ_URL ||
    process.env.MONGO_URL ||
    'mongodb://localhost/prerender';
    
var cacheCollection = process.env.MONGO_CACHE_COLLECTION || 'pages';
var cacheCollectionMeta = process.env.MONGO_CACHE_COLLECTION_META || 'pages_meta';

var database;

MongoClient.connect(mongoUri, function(err, db) {
    if (err) {
        return console.error('ERROR: ' + err.message);
    }
    db.collection(cacheCollectionMeta, function(err, collection) {
        collection.createIndex({ "key": 1 });
        collection.createIndex({ "updated": 1 }, { name: "updated_auto_expire", expireAfterSeconds: 86400 * 90 });
    });
    database = db;
});

var cache_manager = require('cache-manager');

module.exports = {
    init: function() {
        this.cache = cache_manager.caching({
            store: mongo_cache
        });
    },

    beforePhantomRequest: function(req, res, next) {
        if(req.method !== 'GET') {
            return next();
        }

        this.cache.get(req.prerender.url, function (err, result) {
            if (!err && result) {
                res.send(200, result);
            } else {
                next();
            }
        });
    },

    afterPhantomRequest: function(req, res, next) {
        this.cache.set(req.prerender.url, req.prerender.documentHTML, req);
        next();
    }
};


var mongo_cache = {
    get: function(key, callback) {
        database && database.collection(cacheCollection, function(err, collection) {
            if (err) {
                console.error('Error: ' + err.message);
                return callback(err);
            }
            collection.findOne({ key: key }, function (err, item) {
                var value = item ? item.value : null;
                callback(err, value);
            });
        });
    },
    set: function(key, value, request, callback) {
        database && database.collection(cacheCollection, function(err, collection) {
            if (err) {
                return console.error('Error: ' + err.message);
            }
            var object = { 
                $set: { 
                    key: key, 
                    value: value, 
                    created: new Date() 
                } 
            };
            collection.update({ key: key }, object, {
                upsert: true
            }, function (err, res) {
                if (err) {
                    console.error('Error storing the cache results: ' + err.message);
                }
            });
        });
        database && database.collection(cacheCollectionMeta, function(err, collection) {
            if (err) {
                return console.error('Error: ' + err.message);
            }
            var ip = request.headers['x-forwarded-for'] || 
                request.connection.remoteAddress || 
                request.socket.remoteAddress ||
                request.connection.socket.remoteAddress;
            var object = { 
                $setOnInsert: {
                    created: new Date()
                },
                $set: {
                    updated: new Date()
                },
                $push: {
                    requests: {
                        key: key, 
                        occured: request.prerender.start, 
                        status_code: request.prerender.statusCode,
                        execution_time: request.prerender.downloadFinished - request.prerender.downloadStarted,
                        user_agent: request.headers['user-agent'],
                        ip: ip,
                        created: new Date() 
                    }
                } 
            };
            collection.update({ key: key }, object, {
                upsert: true
            }, function (err) {
                if (err) {
                    console.error('Error storing the cache meta results: ' + err.message);
                }
            });
        });
    }
};
