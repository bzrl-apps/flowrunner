const branch = process.env.GITHUB_REF.split("/").pop();

const config = {
  branches: ["master", { name: "next", prerelease: true }],
  plugins: [
    [
      "@semantic-release/commit-analyzer",
      {
        preset: "conventionalcommits",
      },
    ],
    [
      "@semantic-release/release-notes-generator",
      {
        preset: "conventionalcommits",
        presetConfig: {
          types: [
            {
              type: "feat",
              section: "🚀 Features",
            },
            {
              type: "fix",
              section: "🐛 Bug Fixes",
            },
            {
              type: "refactor",
              section: "Refactoring",
            },
            {
              type: "test",
              section: "Tests",
              hidden: true,
            },
            {
              type: "spec",
              section: "Tests",
              hidden: true,
            },
            {
              type: "ci",
              section: "CI",
              hidden: true,
            },
            {
              type: "docs",
              section: "Documentation",
              hidden: true,
            },
            {
              type: "chore",
              section: "Chores",
              hidden: true,
            },
          ],
        },
      },
    ],
  ],
};

if (branch === "master") {
  config.plugins.push("@semantic-release/changelog");
}

config.plugins.push(
  [
    "@semantic-release/exec",
    {
      prepareCmd: ".github/workflows/build.sh ${nextRelease.version}",
    },
  ],
  [
    "@semantic-release/github",
    {
      "assets": [
        {"path": "target/artifacts/flowrunner-*-amd64-linux.tar.gz", "label": "flowrunner-${nextRelease.version}-amd64-linux"},
        {"path": "target/artifacts/flowrunner-*-arm64-linux.tar.gz", "label": "flowrunner-${nextRelease.version}-arm64-linux"},
      ]
    }
  ],
  [
    "@semantic-release/git",
    {
      assets: ["CHANGELOG.md", "Cargo.toml"],
    },
  ]
);

module.exports = config;
