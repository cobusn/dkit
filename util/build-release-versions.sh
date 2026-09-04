#!/usr/bin/env bash
# Build release artifacts for upload to PyPI with twine.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

# Add a version here when the manylinux image contains that interpreter.
# Override these when needed, for example:
#   DKIT_RELEASE_VERSIONS="3.13 3.14" util/build_releases.sh
BUILD_IMAGE=${DKIT_BUILD_IMAGE:-quay.io/pypa/manylinux_2_28_x86_64}
PYTHON_VERSIONS=${DKIT_RELEASE_VERSIONS:-"3.11 3.12 3.13"}
RELEASE_DIR=${DKIT_RELEASE_DIR:-dist}
RELEASE_PATH="${REPO_ROOT}/${RELEASE_DIR}"

if [[ -e "${RELEASE_PATH}" ]] && [[ -n "$(find "${RELEASE_PATH}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Refusing to build into non-empty directory: ${RELEASE_PATH}" >&2
    echo "Choose another DKIT_RELEASE_DIR or remove the old generated artifacts first." >&2
    exit 2
fi
mkdir -p "${RELEASE_PATH}"

echo "Building Python versions ${PYTHON_VERSIONS} in ${BUILD_IMAGE}"
echo "Release artifacts will be written to ${RELEASE_PATH}/"

build_sdist=true
for version in ${PYTHON_VERSIONS}; do
    # CPython 3.11 becomes cp311-cp311, 3.12 becomes cp312-cp312, etc.
    python_tag="cp${version//./}-cp${version//./}"
    python="/opt/python/${python_tag}/bin/python"

    echo "=== Building wheel with Python ${version} (${python}) ==="

    sdist_command=""
    if [[ "${build_sdist}" == true ]]; then
        sdist_command="\"${python}\" -m build --sdist --outdir /io/${RELEASE_DIR};"
    fi

    podman run --rm \
        -v "${REPO_ROOT}:/io" \
        "${BUILD_IMAGE}" \
        bash -c "
            set -euo pipefail
            cd /io
            if [[ ! -x '${python}' ]]; then
                echo 'Python ${version} is not installed in the container' >&2
                exit 2
            fi
            '${python}' -m pip install --disable-pip-version-check build
            ${sdist_command}
            wheel_dir=/tmp/dkit-wheel-${python_tag}
            mkdir -p \"\${wheel_dir}\"
            '${python}' -m build --wheel --outdir \"\${wheel_dir}\"
            wheels=(\"\${wheel_dir}\"/*.whl)
            if [[ \${#wheels[@]} -ne 1 ]]; then
                echo 'Expected exactly one wheel, found' \${#wheels[@]} >&2
                exit 1
            fi
            auditwheel repair \"\${wheels[0]}\" -w '/io/${RELEASE_DIR}'
        "

    build_sdist=false
done

echo "=== Release artifacts ==="
find "${RELEASE_PATH}" -maxdepth 1 -type f -printf '%f\n' | sort
echo "Upload with: python -m twine upload ${RELEASE_DIR}/*"
