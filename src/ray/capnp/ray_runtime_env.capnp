@0x8280c35f6bc39446;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("ray::capnp::core_worker");

struct RuntimeEnvUris {
  # working dir uri
  workingDirUri @0 : Text;
  # python modules uris
  pyModulesUris @1 : List(Text);
}

# The runtime env config, include some fields that do not
# participate in the calculation of the runtime_env hash.
struct RuntimeEnvConfig {
  # The timeout of runtime env creation.
  setupTimeoutSeconds @0 : Int32;
  # Indicates whether to install runtime env eagerly before the workers are leased.
  eagerInstall @1 : Bool;
}

# The runtime env information which is transfered between ray core processes.
struct RuntimeEnvInfo {
  # The serialized runtime env passed from the user.
  serializedRuntimeEnv @0 : Text;
  # URIs used in this runtime env. These will be used for reference counting.
  uris @1 : RuntimeEnvUris;
  # The serialized runtime env config passed from the user.
  runtimeEnvConfig @2 : RuntimeEnvConfig;
}
