var Mongo = require('mongodb'),
    Db = Mongo.Db,
    MongoClient = require('mongodb').MongoClient,
    GridStore = require('mongodb').GridStore,
    Grid = require('gridfs-stream'),
    Stream = require('stream'),
    md5 = require('md5');

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
var historyExpirationTime = process.env.MONGO_CACHE_META_EXPIRATION_TIME || 86400 * 90;

var database, databaseGrid;

MongoClient.connect(mongoUri, function(err, db) {
    if (err) {
        return console.error('ERROR: ' + err.message);
    }
    db.collection(cacheCollectionMeta, function(err, collection) {
        collection.createIndex({ "key": 1 });
        collection.createIndex({ "updated": 1 }, { name: "updated_auto_expire", expireAfterSeconds: historyExpirationTime });
    });
    database = db;
    databaseGrid = Grid(db.db(cacheDatabaseGrid), Mongo);
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
            args.unshift(new Date().toISOString());
            console[type](args.join(' '));
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
                res.send(result.statusCode, result.value);
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
        if (!databaseGrid) {
            return callback(null, null);
        }
        var _this = this;
        var _id = md5(key);
        // GridStore doesn't allow exist on string _id, it treats it as a filename
        // http://stackoverflow.com/a/14321950/266561
        // https://mongodb.github.io/node-mongodb-native/api-generated/gridstore.html#gridstore-exist
        databaseGrid.exist({
            filename: key
        }, function(err, found) {
            if (err || !found) {
                _this.log(request, 'Cache file for ' + _id + ' - ' + key + ' doesn\'t exist');
                // check if there's an error request saved in request history, and return that one
                database && database.collection(cacheCollectionMeta, function(err, collection) {
                    if (err) {
                        _this.error(request, 'Error: ' + err.message);
                        return callback(err, null);
                    }
                    // get the last request in history
                    var req = collection.findOne({ key: key }, { requests: { $slice: -1 } }, function(err, res) {
                        if (err) {
                            _this.error(request, 'Error: ' + err.message);
                            return callback(err, null);
                        }
                        if (!res || !res.requests) {
                            _this.log(request, 'Cache history for ' + key + 'doesn\'t exist');
                            return callback(null, null);
                        }
                        callback(null, res.requests[0]);
                    });
                });
            } else {
                _this.log(request, 'Found gridFS file ' + _id + ' - ' + key);
                callback(null, {
                    statusCode: 200,
                    value: databaseGrid.createReadStream({
                        _id: _id
                    })
                });
            }
        });
    },
    set: function(request, key, value, callback) {
        var _this = this;
        var _id = md5(key);
        // only save successful response
        if (request.prerender.statusCode && String(request.prerender.statusCode).match(/^2.*/)) {
            // 2xx response
            database && database.collection(cacheCollection, function(err, collection) {
                if (err) {
                    return _this.error(request, 'Error: ' + err.message);
                }
                var object = {
                    $set: {
                        response_time: request.prerender.downloadFinished - request.prerender.downloadStarted
                    },
                    $setOnInsert: {
                        _id: _id,
                        key: key,
                        created: new Date()
                    }
                };
                collection.update({ _id: _id }, object, { upsert: true }, function (err) {
                    if (err) {
                        _this.error(request, 'Error storing the cache results: ' + err.message);
                    }
                });
            });

            var writeStream = databaseGrid.createWriteStream({
                _id: _id,
                uploadDate: new Date(),
                filename: key
            });
            writeStream.on('error', function(err) {
                _this.error(request, 'Error: ' + err.message);
            });
            writeStream.on('close', function(err, res) {
                _this.log(request, 'Written cache results for ' + key + ' to ' + _id);
                // suggested way for updating metadata with gridfs-stream
                databaseGrid.files.update({
                    _id: _id
                }, { 
                    $set: {
                        uploadDate: new Date()
                    }
                }, function(err, updated) {
                    if (err) {
                        _this.error(request, 'Error updating uploadDate for ' + key);
                    }
                });
            });
            var s = new Stream.Readable();
            s.push(value);
            s.push(null);
            s.pipe(writeStream);
            s.on('error', function(err) {
                _this.error(request, 'Error: ' + err.message);
            });
        }

        database && database.collection(cacheCollectionMeta, function(err, collection) {
            if (err) {
                return _this.error(request, 'Error: ' + err.message);
            }

            var ip = request.headers['x-forwarded-for'] || 
                request.connection.remoteAddress || 
                request.socket.remoteAddress ||
                request.connection.socket.remoteAddress;
            
            var historyObj = {
                key: key, 
                occured: request.prerender.start, 
                status_code: request.prerender.statusCode,
                execution_time: request.prerender.downloadFinished - request.prerender.downloadStarted,
                user_agent: request.headers['user-agent'],
                ip: ip,
                created: new Date() 
            };
            if (!request.prerender.statusCode || !String(request.prerender.statusCode).match(/^2.*/)) {
                // we might 
                historyObj.statusCode = request.prerender.statusCode;
                historyObj.value = value;
            }

            var object = { 
                $setOnInsert: {
                    _id: _id,
                    key: key,
                    created: new Date()
                },
                $set: {
                    updated: new Date()
                },
                $push: {
                    requests: historyObj
                }
            };
            collection.update({ _id: _id }, object, {
                upsert: true
            }, function (err, result) {
                if (err) {
                    _this.error(request, 'Error storing the cache meta results: ' + err.message);
                }
            });
        });
    }
};