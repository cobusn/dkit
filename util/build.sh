#! /bin/bash
# build distributions ready for upload to pip
#
set -eu

VERSION=${1:-3.13}
TAG=${VERSION//./}

echo "Building for version: $VERSION with tag $TAG"
podman run --rm -v "$PWD:/io" quay.io/pypa/manylinux_2_28_x86_64 \
bash -c "cd /io && /opt/python/cp${TAG}-cp${TAG}/bin/pip install build && /opt/python/cp${TAG}-cp${TAG}/bin/python -m build --sdist --wheel && auditwheel repair dist/*.whl -w dist/"

echo "Check with: python -m twine check dist/*"
echo "Upload with: python -m twine upload dist/*"
