'use strict';

var Job = require('kue').Job;
var Promise = require('bluebird').Promise;
var _ = require('lodash');
var kue = require('kue');
var noop = require('node-noop').noop;


const baseGet = Job.get;
const baseRemove = Job.prototype.remove;
const baseSave = Job.prototype.save;

const SYM_UNIQUEDATA = Symbol('unique-data');
const JOB_IDENTIFIERS = 'unique:jobs';
const JOB_ASSOCIATIONS = 'unique:associations';


function _flatten(obj, prefix) {
	var result;

	result = {};

	for (let key in obj) {
		if (!obj.hasOwnProperty(key)) {
			break;
		}

		let value = obj[key];
		let property = prefix ? prefix + '.' : '';

		if (_.isPlainObject(value)) {
			let extension = _flatten(value, property + key);

			for (let flattened in extension) {
				if (!extension.hasOwnProperty(flattened)) {
					break;
				}

				result[flattened] = extension[flattened];
			}
		}
		else {
			result[property + key] = value;
		}
	}

	return result;
}


function _getUniqueIdentifier(obj) {
	if (!_.isPlainObject(obj)) {
		return String(obj);
	}
	else {
		obj = _flatten(obj);
		let aggregate = '';
		let keys = Object.keys(obj).sort();

		for (let i = 0; i < keys.length; i++) {
			let key = keys[i];

			aggregate += key + ':' + obj[key];

			if (i < keys.length - 1) {
				aggregate += '::';
			}
		}

		return aggregate;
	}
}


Job.get = function get(id, type, callback) {
	var promise, self = this;

	if (typeof type === 'function' && !callback) {
		callback = type;
		type = '';
	}

	callback = callback || noop;

	promise = new Promise(function(resolve, reject) {
		baseGet.call(self, id, type, function(err, job) {
			if (err) {
				reject(err);
			}
			else {
				resolve(job);
			}
		});
	});

	promise
		.then(function(job) {
			let assocKey = Job.client.getKey(JOB_ASSOCIATIONS);

			return Job.client.hget(assocKey, id)
				.then(function(identifier) {
					let identKey = Job.client.getKey(JOB_IDENTIFIERS);

					if (identifier) {
						return Job.client.hget(identKey, identifier)
							.then(function(data) {
								job[SYM_UNIQUEDATA] = JSON.parse(data);

								return Promise.resolve(job);
							});
					}

					return Promise.resolve(job);
				});
		})
		.then(function(job) {
			callback(null, job);
		})
		.catch(function(err) {
			callback(err, null);
		});
};


_.assign(Job.prototype, {
	remove: function Job$remove(callback) {
		var assocKey, id, promise, identKey, self = this;

		callback = callback || noop;
		id = this.id;

		promise = new Promise(function(resolve, reject) {
			baseRemove.call(self, function(err) {
				if (err) {
					reject(err);
				}
				else {
					resolve();
				}
			});
		});

		promise
			.then(function() {
				identKey = Job.client.getKey(JOB_IDENTIFIERS);
				assocKey = Job.client.getKey(JOB_ASSOCIATIONS);

				return Job.client.hget(assocKey, id);
			})
			.then(function(identifier) {
				return Job.client.multi()
					.hdel(assocKey, id)
					.hdel(identKey, identifier)
					.exec();
			})
			.then(function() {
				callback(null, self);
			})
			.catch(function(err) {
				callback(err, self);
			});
	},

	save: function Job$save(callback) {
		var identifier, promise, update, self = this;

		callback = callback || noop;
		update = !this.id && this.hasOwnProperty(SYM_UNIQUEDATA);

		if (update) {
			let key = Job.client.getKey(JOB_IDENTIFIERS);
			let data = this[SYM_UNIQUEDATA];
			identifier = _getUniqueIdentifier(data);

			promise = Job.client.hsetnx(key, identifier, JSON.stringify(data))
				.then(function(result) {
					if (result === 0) {
						return Promise.reject(new Error('Duplicate unique identifier'));
					}

					return Promise.resolve(null);
				});
		}
		else {
			promise = Promise.resolve(null);
		}

		promise = promise.then(function() {
			return new Promise(function(resolve, reject) {
				baseSave.call(self, function(err) {
					if (err) {
						reject(err);
					}
					else {
						resolve();
					}
				});
			});
		});

		if (update) {
			promise = promise.then(function() {
				let key = Job.client.getKey(JOB_ASSOCIATIONS);

				return Job.client.hset(key, self.id, identifier);
			});
		}

		promise
			.then(function() {
				callback(null, self);
			})
			.catch(function(err) {
				callback(err, self);
			});

		return this;
	},

	unique: function Job$unique(data) {
		if (this.hasOwnProperty(SYM_UNIQUEDATA)) {
			throw new Error('Cannot change a jobs unique identifier');
		}

		this[SYM_UNIQUEDATA] = _.cloneDeep(data);

		return this;
	}
});


module.exports = kue;
