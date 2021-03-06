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
                res.send(result.statusCode, result.value, result.hash);
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
        databaseGrid.findOne({
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
                    var req = collection.findOne({ key: key }, { status_code: 1, value: 1, hash: 1, contentType: 1 }, function(err, res) {
                        if (err) {
                            _this.error(request, 'Error: ' + err.message);
                            return callback(err, null);
                        }
                        if (!res) {
                            _this.log(request, 'Cache history for ' + key + 'doesn\'t exist');
                            return callback(null, null);
                        }
                        if (String(res.status_code).match(/^2.*/) && res.value) {
                            _this.log(request, 'Returning last valid cache from ' + cacheCollectionMeta);
                            return callback(null, {
                                statusCode: res.status_code,
                                value: res.value,
                                hash: res.hash ? res.hash : md5(res.value),
                                contentType: res.contentType ? res.contentType : ''
                            });
                        }
                        callback(null, null);
                    });
                });
            } else {
                _this.log(request, 'Found gridFS file ' + _id + ' - ' + key);
                var readStream = databaseGrid.createReadStream({
                    _id: _id
                });
                readStream.on('open', function() {
                    callback(null, {
                        statusCode: 200,
                        value: readStream,
                        hash: found.metadata && found.metadata.hash ? found.metadata.hash : '',
                        contentType: found.metadata && found.metadata.contentType ? found.metadata.contentType : ''
                    });
                });
                readStream.on('error', function(err) {
                    callback(err, null);
                });
            }
        });
    },
    set: function(request, key, value, callback) {
        var _this = this;
        var _id = md5(key);
        var now = new Date();
        // only save successful response
        if (request.prerender.statusCode && String(request.prerender.statusCode).match(/^2.*/)) {
            // 2xx response
            database && database.collection(cacheCollection, function(err, collection) {
                if (err) {
                    return _this.error(request, 'Error: ' + err.message);
                }
                var object = {
                    $set: {
                        response_time: request.prerender.downloadFinished - request.prerender.downloadStarted,
                        status: 'processed',
                        status_code: request.prerender.statusCode,
                        processed: now
                    },
                    $setOnInsert: {
                        _id: _id,
                        key: key,
                        sitemap_url: request.headers && request.headers['user-agent'] ? request.headers['user-agent'] : '',
                        created: now
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
                uploadDate: now,
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
                        uploadDate: now,
                        metadata: {
                            hash: md5(value),
                            contentType: request.prerender.contentType
                        }
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
                (request.connection ? request.connection.remoteAddress : null) || 
                (request.socket ? request.socket.remoteAddress : null) ||
                (request.connection.socket ? request.connection.socket.remoteAddress : null);
            
            var historyObj = {
                key: key, 
                occured: request.prerender.start, 
                status_code: request.prerender.statusCode,
                contentType: request.prerender.contentType,
                execution_time: request.prerender.downloadFinished - request.prerender.downloadStarted,
                user_agent: request.headers['user-agent'],
                ip: ip,
                created: now
            };
            var $set = {
                updated: now
            };
            if (request.prerender.statusCode && String(request.prerender.statusCode).match(/^2.*/)) {
                // we might cache the value here for future requests
                $set.status_code = request.prerender.statusCode;
                $set.contentType = request.prerender.contentType;
                $set.value = value;
                $set.hash = md5(typeof value === 'string' ? value : '');
            }

            var object = { 
                $setOnInsert: {
                    _id: _id,
                    key: key,
                    created: now
                },
                $set: $set,
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