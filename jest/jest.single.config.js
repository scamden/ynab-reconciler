const config = require('./jest.config');
config.roots = ['../dist'];
config.snapshotResolver = './single.snapshotResolver.js';
module.exports = config;