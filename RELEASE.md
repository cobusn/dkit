# Releasing DKit

Run all release commands from the repository root.

## Prepare the release

1. Make sure the working tree is understood and the previous release tag is
   available:

   ```bash
   git status --short
   git tag --sort=-version:refname | head
   ```

2. Update `dkit.__version__` in `dkit/__init__.py`. Use the package format
   `YY.M.PATCH`, for example `26.10.1`.

3. Generate a release-note draft from the previous tag to the current commit:

   ```bash
   util/release-notes.sh v26.09.1 HEAD > /tmp/release-notes.md
   ```

   Review the draft and add the user-facing entries to the top of
   `HISTORY.md`. Remove internal-only or duplicate entries as appropriate.

4. Run the test suite and commit the version and history changes:

   ```bash
   make test
   git add dkit/__init__.py HISTORY.md
   git commit -m "chore: prepare release 26.10.1"
   ```

The release commit must contain the `HISTORY.md` update before it is tagged.

## Tag the release

Create the tag from the release commit. Package versions use an unpadded
month, while Git tags use a zero-padded month:

```bash
version=$(python3 -c 'import dkit; print(dkit.__version__)')
tag=$(python3 -c \
    'import dkit; y, m, p = dkit.__version__.split("."); \
     print(f"v{y}.{int(m):02d}.{p}")')
git tag -a "$tag" -m "release $tag"
```

Verify that the tag and package agree:

```bash
git show "$tag:dkit/__init__.py" | grep __version__
git describe --exact-match --tags HEAD
```

## Build and publish

Build and inspect the distributions:

```bash
make clean
make build
python3 -m twine check dist/*
```

For the supported manylinux wheel build, use the release build utility:

```bash
util/build-release-versions.sh
```

Upload only after reviewing the generated files:

```bash
python3 -m twine upload dist/*
```

Finally, publish the tag and confirm the release on the package index:

```bash
git push origin "$tag"
```

## Compare releases

To generate notes for an already-tagged release range:

```bash
util/release-notes.sh v26.09.1 v26.10.1
```
