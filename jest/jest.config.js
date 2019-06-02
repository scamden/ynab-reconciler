const transformTsPaths = require('transform-ts-paths');
const tsConfig = require('../tsconfig.json');
const jestTransforms = transformTsPaths.jestTransforms;
const transformPaths = transformTsPaths.transformPaths;

const moduleNameMapper = Object.assign({}, transformPaths(
  tsConfig,
  jestTransforms.alias,
  jestTransforms.path
));

module.exports = {
  preset: 'ts-jest',
  verbose: true,
  testEnvironment: 'node',
  roots: ['../src'],
  testPathIgnorePatterns: ['\\.d\\.ts$'],
  moduleFileExtensions: [
    'ts',
    'tsx',
    'js'
  ],
  moduleNameMapper: moduleNameMapper,
  watchPathIgnorePatterns: ['/node_modules/', '/dist/'],
  setupFiles: ['./setup.js'],
  snapshotResolver: './snapshotResolver.js',
};