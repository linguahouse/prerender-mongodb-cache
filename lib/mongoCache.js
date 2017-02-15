var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    GridStore = require('mongodb').GridStore,
    Grid = require('mongodb').Grid;

var mongoUri = process.env.MONGOLAB_URI ||
    process.env.MONGOHQ_URL ||
    process.env.MONGO_URL ||
    'mongodb://localhost/prerender';
    
// collection name for sitemap entries
var cacheCollection = process.env.MONGO_CACHE_COLLECTION || 'pages';
// collection name for each sitemap entry request history
var cacheCollectionMeta = process.env.MONGO_CACHE_COLLECTION_META || 'pages_meta';
// GridFS database name for cached documents
var cacheDatabaseGrid = process.env.MONGO_CACHE_DATABASE_GRID || 'pages_grid';

var database, databaseGrid;

MongoClient.connect(mongoUri, function(err, db) {
    if (err) {
        return console.error('ERROR: ' + err.message);
    }
    db.collection(cacheCollectionMeta, function(err, collection) {
        collection.createIndex({ "key": 1 });
        collection.createIndex({ "updated": 1 }, { name: "updated_auto_expire", expireAfterSeconds: 86400 * 90 });
    });
    database = db;
    databaseGrid = db.db(cacheDatabaseGrid);
});

var cache_manager = require('cache-manager');

module.exports = {
    init: function() {
        this.cache = cache_manager.caching({
            store: mongo_cache,
        });
        this.cache.console = function(type, args) {
            var request = args.shift();
            args.unshift(request.request_id);
            console[type].apply(console[type], [new Date().toISOString()].concat(args));
        };
        this.cache.log = function() {
            this.console('log', Array.prototype.slice.call(arguments));
        }.bind(this.cache);
        this.cache.error = function() {
            this.console('error', Array.prototype.slice.call(arguments));
        }.bind(this.cache);
    },

    beforePhantomRequest: function(req, res, next) {
        if(req.method !== 'GET') {
            return next();
        }

        this.cache.get(req, req.prerender.url, function (err, result) {
            if (!err && result) {
                res.send(200, result);
            } else {
                next();
            }
        });
    },

    afterPhantomRequest: function(req, res, next) {
        this.cache.set(req, req.prerender.url, req.prerender.documentHTML);
        next();
    }
};


var mongo_cache = {
    get: function(request, key, callback) {
        var _this = this;
        database && database.collection(cacheCollectionMeta, function(err, collection) {
            if (err) {
                _this.error(request, 'database.collection(cacheCollectionMeta) Error: ' + err.message);
                return callback(err);
            }
            collection.findOne({ key: key }, { _id: 1 }, function (err, item) {
                if (err) {
                    _this.error(request, 'collection.findOne Error: ' + err.message);
                    return callback(err, null);
                } else if (!item) {
                    return callback(err, item);
                }
                gridStore = new GridStore(databaseGrid, item._id, "r");
                gridStore.open(function(err, gs) {
                    if (err) {
                        // file doesn't exist
                        _this.log(request, 'Cache file for ' + item._id + ' doesn\'t exist');
                        return callback(err, null);
                    }
                    _this.log(request, 'Found gridFS file ' + gs.fileId);
                    var stream = gs.stream(true);
                    callback(err, stream);
                });
            });
        });
    },
    set: function(request, key, value, callback) {
        var _this = this;
        database && database.collection(cacheCollection, function(err, collection) {
            if (err) {
                return _this.error(request, 'Error: ' + err.message);
            }
            var object = { 
                key: key,
                created: new Date()
            };
            collection.insert(object, function (err) {
                if (err) {
                    _this.error(request, 'Error storing the cache results: ' + err.message);
                }
            });
        });

        database && database.collection(cacheCollectionMeta, function(err, collection) {
            if (err) {
                return _this.error(request, 'Error: ' + err.message);
            }

            var ip = request.headers['x-forwarded-for'] || 
                request.connection.remoteAddress || 
                request.socket.remoteAddress ||
                request.connection.socket.remoteAddress;
            
            var object = { 
                $setOnInsert: {
                    key: key,
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
            collection.findAndModify({ key: key }, [['_id','asc']], object, {
                upsert: true,
                new: true
            }, function (err, result) {
                if (err) {
                    _this.error(request, 'Error storing the cache meta results: ' + err.message);
                } else {
                    gridStore = new GridStore(databaseGrid, result.value._id, key, "w");
                    gridStore.open(function(err, gridStore) {
                        if (err) {
                            return _this.error(request, 'Error: ' + err.message);
                        }
                        gridStore.write(value, function(err, gridStore) {
                            if (err) {
                                return _this.error(request, 'Error while writing to gridFS: ' + err.message);
                            }
                            gridStore.close(function(err, fileData) {
                                if (err) {
                                    return _this.error(request, 'Error while flushing to gridFS: ' + err.message);
                                } else {
                                    _this.log(request, 'Cache for ' + key + ' saved under ' + result.value._id);
                                }
                            });
                        });
                    });
                }
            });
        });
    }
};
