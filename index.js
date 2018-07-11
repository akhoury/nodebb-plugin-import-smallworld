
var async = require('async');
var extend = require('extend');
var mysql = require('mysql');
var _ = require('underscore');
var moment = require('moment');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-smallworld]';
var parseTillObject = function (str) {
	if (typeof str !== 'string') {
		return str;
	}
	try {
		return parseTillObject(JSON.parse(str));
	} catch (e) {}
};

(function(Exporter) {

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'root',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || 'smallworld'
		};

		Exporter.log(_config);

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		config.custom = config.custom || {};
		if (typeof config.custom === 'string') {
			try {
				config.custom = JSON.parse(config.custom)
			} catch (e) {}
		}

		config.custom = config.custom || {};
		config.custom.timemachine = config.custom.timemachine || {};
		config.custom = extend(true, {}, {
			/* TODO: ADD TIMEMACHINE SUPPORT */
		}, config.custom);

		Exporter.config('custom', config.custom);

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		setInterval(function() {
			Exporter.connection.query("SELECT 1", function(){});
		}, 60000);

		callback(null, Exporter.config());
	};

	Exporter.query = function (query, callback) {
		console.log('\n==========<query>============');
		console.log(query);
		console.log('==========</query>============\n');
		return Exporter.connection.query(query, callback);
	};

	Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback)
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		Exporter.log('getPaginatedGroups');
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '
			+ '\n' +  prefix + 'groups.id as _gid, '
			+ '\n' +  prefix + 'groups.group_name as _name, '
			+ '\n' +  prefix + 'groups.description as _description, '
			+ '\n IF(' + prefix + 'groups.deleted_at IS NULL, 0, 1) as _deleted, '
			+ '\n' +  prefix + 'groups.slug as _slug, '
			+ '\n' +  prefix + 'forum_categories.id as _cids, '
			+ '\n' +  prefix + 'groups.owner_id as _ownerUid '
			+ '\n' +  'FROM ' + prefix + 'groups '
			+ '\n' + 'LEFT JOIN ' + prefix + 'forum_categories ON ' + prefix + 'forum_categories.group_id = ' + prefix + 'groups.id '
			+ '\n' + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		getUserIdsMap(function (err, idsMap) {
			Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					//normalize here
					var map = {};
					rows.forEach(function(row) {
						if (row._name) {
							row._name = row._name
								.replace(/\//g, '-')
								.replace(/:/g, '-');
						}
						if (row._cids) {
							row._cids = [].concat(row._cids);
						} else {
							delete row._cids;
						}
						try {
							row._ownerUid = JSON.parse(row._ownerUid)[0];
						} catch (e) {
							delete row._ownerUid;
						}
						if (row._ownerUid) {
							row._ownerUid = idsMap[row._ownerUid] ? idsMap[row._ownerUid]._uid : null;
						}
						map[row._gid] = row;
					});
					callback(null, map);
				});
		});
	};



	var getUserIdsMap = function (callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');

		if (Exporter['_users_ids_map_']) {
			return callback(null, Exporter['_users_ids_map_']);
		}

		var query = 'SELECT '
			+ '\n' + prefix + 'users.id as _id, '
			+ '\n' + prefix + 'users.small_world_id as _uid '
			+ '\n' + 'FROM ' + prefix + 'users '
			+ '\n' + 'ORDER BY ' + prefix + 'users.id ';
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				var map = {};
				rows.forEach(function (row) {
					map[row._id] = row;
				});
				Exporter['_users_ids_map_'] = map;
				callback(null, map);
			});
	};

	Exporter.getUsers = function(callback) {
		Exporter.getPaginatedUsers(0, -1, callback);
	};

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		Exporter.log('getPaginatedUsers');
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '

			// weird, looks like small_world_id is the uid here used with the topics and posts
			+ '\n' + prefix + 'users.id as _id, '
			+ '\n' + prefix + 'users.small_world_id as _uid, '

			+ '\n' + prefix + 'users.username as _username, '
			+ '\n' + prefix + 'users.display_name as _fullname, '
			+ '\n' + prefix + 'users.email as _email, '
			+ '\n' + prefix + 'users.created_at as _joindate, '
			+ '\n CONCAT(' + prefix + 'users.city, \',\', ' + prefix + 'users.state) AS _location, '
			+ '\n' + prefix + 'users.avatar as _picture, '
			+ '\n' + prefix + 'users.groups as _groups, '
			+ '\n' + prefix + 'users.connections as _followingUids, '
			+ '\n' + prefix + 'users.connections as _friendsUids, '
			+ '\n' + prefix + 'forum_posts.created_at as _firstPostDate '

			+ '\n' + 'FROM ' + prefix + 'users '
			+ '\n' + 'LEFT JOIN ' + prefix + 'forum_posts ON ' + prefix + 'forum_posts.sw_user_id = ' + prefix + 'users.id '
			+ '\n' + 'ORDER BY ' + prefix + 'users.id '
			+ '\n' + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		getUserIdsMap(function (err, idsMap) {
			Exporter.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						// some users don't have a small_world_id
						if (!row._uid) {
							row._uid = 'i' + row._id;
						}
						if (!row._email || !/\S+@\S+/.test(row._email)) {
							row._email = 'INVALID_EMAIL+' + row._email + '@NODEBB.ORG';
						}
						if (row._groups) {
							row._groups = parseTillObject(row._groups);
						}
						if (row._followingUids) {
							row._followingUids = parseTillObject(row._followingUids);

							if (row._followingUids) {
								row._followingUids = row._followingUids
									.map(function (id) {
										return idsMap[id] ? idsMap[id]._uid : null;
									})
									.filter(function (_uid) {
										return !!_uid;
									});
							}
						}
						if (row._friendsUids) {
							row._friendsUids = parseTillObject(row._friendsUids);
							if (row._friendsUids) {
								row._friendsUids = row._friendsUids
									.map(function (id) {
										return idsMap[id] ? idsMap[id]._uid : null;
									})
									.filter(function (_uid) {
										return !!_uid;
									});
							}
						}
						if (row._joindate) {
							row._joindate = moment(row._joindate).valueOf();
						}
						if (!row._joindate && row._firstPostDate) {
							row._joindate = moment(row._firstPostDate).valueOf();
						}
						map[row._uid] = row;
					});

					callback(null, map);
				});
		});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		Exporter.log('getPaginatedCategories');
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '
			+ '\n' +  prefix + 'forum_categories.id as _cid, '
			+ '\n' +  prefix + 'forum_categories.parent_id as _parentCid, '
			+ '\n' +  prefix + 'forum_categories.title as _name, '
			+ '\n' +  prefix + 'forum_categories.description as _description, '
			+ '\n' +  prefix + 'forum_categories.created_at as _timestamp, '
			+ '\n' +  prefix + 'forum_categories.slug as _slug '
			+ '\n' +  'FROM ' + prefix + 'forum_categories '
			+ '\n' + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				var map = {};
				rows.forEach(function(row) {
					if (row._timestamp) {
						row._timestamp = moment(row._timestamp).valueOf();
					}
					map[row._cid] = row;
				});

				callback(null, map);
			});
	};


	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};

	Exporter.getPaginatedTopics = function(start, limit, callback) {
		Exporter.log('getPaginatedTopics');
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '
			+ '\n' +  prefix + 'forum_topics.id as _tid, '
			+ '\n' +  prefix + 'forum_topics.category as _cid, '
			+ '\n' +  prefix + 'forum_topics.sw_user_id as _uid, '
			+ '\n' +  prefix + 'forum_topics.first_post_id as _mainPid, '
			+ '\n' +  prefix + 'forum_topics.title as _title, '
			+ '\n' +  prefix + 'forum_topics.created_at as _timestamp, '
			+ '\n' +  prefix + 'forum_topics.updated_at as _edited, '
			+ '\n IF(' + prefix + 'forum_topics.deleted_at IS NULL, 0, 1) as _deleted, '
			+ '\n' +  prefix + 'forum_posts.content as _content, '
			+ '\n' +  prefix + 'forum_topics.slug as _slug, '
			+ '\n' +  prefix + 'forum_topics.url as _url '

			+ '\n' +  'FROM ' + prefix + 'forum_topics '
			+ '\n' + 'JOIN ' + prefix + 'forum_posts ON ' + prefix + 'forum_posts.id = ' + prefix + 'forum_topics.first_post_id '

			+ '\n' + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.query(query,
			function(err, rows) {
				var map = {};
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				rows.forEach(function(row) {
					if (row._timestamp) {
						row._timestamp = moment(row._timestamp).valueOf();
					}
					if (row._edited) {
						row._edited = moment(row._edited).valueOf();
					}
					map[row._tid] = row;
				});
				callback(null, map);
			});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback)
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		Exporter.log('getPaginatedPosts');
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT '
			+ prefix + 'forum_posts.id as _pid, '
			+ '\n' +  prefix + 'forum_posts.topic_id as _tid, '
			+ '\n' +  prefix + 'forum_posts.sw_user_id as _uid, '
			+ '\n' +  prefix + 'forum_posts.content as _content, '
			+ '\n' +  prefix + 'forum_posts.created_at as _timestamp, '
			+ '\n' +  prefix + 'forum_posts.updated_at as _edited, '
			+ '\n' +  prefix + 'forum_posts.url as _url '
			+ '\n FROM ' + prefix + 'forum_posts '
			+ '\n WHERE ' + prefix + 'forum_posts.id NOT IN (SELECT first_post_id FROM ' + prefix + 'forum_topics) '
			+ '\n' + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.query(query,
			function(err, rows) {
				var map = {};
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				rows.forEach(function(row) {
					if (row._timestamp) {
						row._timestamp = moment(row._timestamp).valueOf();
					}
					if (row._edited) {
						row._edited = moment(row._edited).valueOf();
					}
					map[row._pid] = row;
				});
				callback(null, map);
			});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getPaginatedGroups(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedUsers(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	var csvToArray = function(v) {
		return !Array.isArray(v) ? ('' + v).split(',').map(function(s) { return s.trim(); }) : v;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);
