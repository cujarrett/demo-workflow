# demo-workflow

The sample workflow app that Launchpad guest sandboxes run. One image, two replicas, each a worker that also serves a small public API.

## Rules

- **Never run `git add`, `git commit`, `git push`, or any git command that writes to the index, history or remotes.** Output the commands for the user to run.
- **Never add a `Co-Authored-By` trailer or a "Generated with Claude Code" line** to commit messages or PR descriptions.
- **Always give a suggested commit message** when a task needs a commit. Give `git add`, with every file named, and the commit as two separate steps.
- **Never include environment variable values, file contents or internal paths in HTTP responses.** Public guests call this app.

### Pre-commit safety check

Before telling the user to commit, run `/security-review`. Once it says the changes are safe, offer a suggested commit message.

## Philosophy: Grug-Brained Development

> "Complexity very, very bad." ([grugbrain.dev](https://grugbrain.dev/))

- **Say no.** No feature or abstraction until it earns its place.
- **Cheapest rung that works.** Skip the feature, reuse code already here, standard library, a dependency already installed, then build the minimum.
- **Boring, obvious code wins.**
- **Comments say why, in two or three lines.**

## Go

- Stdlib first. Add a dependency only when it is the official client, such as the Temporal and Azure SDKs.
- Use `just`, never `make`. Run `just ci` before pushing.
