## About
This image is an extension of the [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) image. It includes all extended requirements of `RLlib`, `Serve` and `Tune`. It is a well-provisioned starting point for trying out the Ray ecosystem. [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray-ml/Dockerfile)

## Tags

Images are `tagged` with the format `{Ray version}[-{Python version}][-{Platform}]`. `Ray version` tag can be one of the following:

| Ray version tag | Description |
| --------------- | ----------- |
| `latest`                     | The most recent Ray release. |
| `x.y.z`                      | A specific Ray release, e.g. 2.9.3 |
| `nightly`                    | The most recent Ray development build (a recent commit from GitHub `master`) |

The optional `Python version` tag specifies the Python version in the image. All Python versions supported by Ray are available, e.g. `py39`, `py310` and `py311`. If unspecified, the tag points to an image using `Python 3.9`.

The optional `Platform` tag specifies the platform where the image is intended for:

| Platform tag | Description |
| --------------- | ----------- |
| `-cpu`  | These are based off of an Ubuntu image. |
| `-cuXX` | These are based off of an NVIDIA CUDA image with the specified CUDA version `xx`. They require the NVIDIA Docker Runtime. |
| `-gpu`  | Aliases to a specific `-cuXX` tagged image. |
| no tag  | Aliases to `-cpu` tagged images for `ray`, and aliases to ``-gpu`` tagged images for `ray-ml`. |

Examples tags:
- none: equivalent to `latest`
- `latest`: equivalent to `latest-py39-gpu`, i.e. image for the most recent Ray release
- `nightly-py39-cpu`
- `806c18-py39-cu112`

The `ray-ml` images are not built for the `arm64` (`aarch64`) architecture.

## Other Images
* [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) - Ray and all of its dependencies.
