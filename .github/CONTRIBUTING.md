# Contribution Guidelines

## Questions

The GitHub issue tracker is for *bug reports* and *feature requests*. Please do
not use it to ask questions about how to use Pebble. These questions should
instead be directed to [Stack Overflow](https://stackoverflow.com/).

## Bug Reports

Please avoid raising duplicate issues. Use GitHub issue search feature
to check whether your bug report or feature request has been mentioned in
the past.

When filing bug reports about exceptions or tracebacks, please include the
*complete* traceback.

Since Pebble provides APIs very similar to those of `threading`,
`multiprocessing` and `concurrent.futures` and builds upon those modules,
it is recommended to first check whether said modules show the same
behaviour before reporting it as a Pebble issue.

Please provide means to reproduce the issue. Without it, it becomes very difficult
to troubleshoot the problem.

Provide detailed information on **what you expected to happen** and **what actually
happens instead**.

Provide information on your OS, its version, Python and Pebble version.

## Pull Requests

As per the issues, avoid raising duplicate pull requests. Use GitHub pull request
search feature to check whether someone else has already filed a pull request.

Provide tests reproducing the issue you are fixing or the feature
you are implementing to prevent regressions.

Always run the test suite and the type checking locally. You can inspect the GitHub
workflows to see how to do so.
