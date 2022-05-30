module.exports = {
  plugins: [
    '@semantic-release/commit-analyzer',
    '@semantic-release/release-notes-generator',
    ['@semantic-release/changelog', {
      changelogFile: 'CHANGELOG.md',
    }],
    ["@semantic-release/npm", {
      npmPublish: false,
    }],
    ['@semantic-release/git', {
      assets: ['package.json', 'package-lock.json', 'CHANGELOG.md'],
      message: 'chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}'
    }],
    ['@semantic-release/exec', {
      publishCmd: 'npm run deploy'
    }],
    '@semantic-release/github',
    ['@adobe/semantic-release-coralogix', {
      iconUrl: 'https://emojipedia-us.s3.dualstack.us-west-1.amazonaws.com/thumbs/240/apple/325/ship_1f6a2.png',
      'subsystems': ['helix-services'],
      'applications': ['lqmig3v5eb'],
    }]
  ],
  branches: ['main'],
};
