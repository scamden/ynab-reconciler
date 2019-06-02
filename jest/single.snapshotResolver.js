const snapShotResolver = require('./getSnapshotResolver')('dist/');
module.exports = {
  ...snapShotResolver,
  resolveSnapshotPath: (testPath, snapshotExtension) => {
    return snapShotResolver.resolveSnapshotPath(testPath, snapshotExtension)
      .replace('.js', '.ts')
  },
  resolveTestPath: (snapshotFilePath, snapshotExtension) => {
    return snapShotResolver.resolveTestPath(snapshotFilePath, snapshotExtension)
      .replace('.ts', '.js')
  }
};
