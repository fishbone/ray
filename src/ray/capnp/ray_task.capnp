@0xb023e78ccc2809bd;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("ray::capnp::core_worker");

using import "ray_runtime_env.capnp".RuntimeEnvInfo;

struct Map(Key, Value) {
  entries @0 : List(Entry);
  struct Entry {
    key @0 : Key;
    value @1 : Value;
  }
}

enum TaskType {
  # Normal task.
  kNormalTask @0;
  # Actor creation task.
  kActorCreationTask @1;
  # Actor task.
  kActorTask @2;
  # Driver task.
  kDriverTask @3;
}

enum Language {
  kPython @0;
  kJava @1;
  kCpp @2;
}

struct ObjectReference {
  # ObjectID that the worker has a reference to.
  objectId @0 : Data;
  # The address of the object's owner.
  ownerAddress @1 : Address;
  # Language call site of the object reference (i.e., file and line number).
  # Used to print debugging information if there is an error retrieving the
  # object.
  callSite @2 : Text;
}

# Argument in the task.
struct TaskArg {
  # A pass-by-ref argument.
  objectRef @0 : ObjectReference;
  # Data for pass-by-value arguments.
  data @1 : Data;
  # Metadata for pass-by-value arguments.
  metadata @2 : Data;
  # ObjectIDs that were nested in the inlined arguments of the data field.
  nestedInlinedRefs @3 : List(ObjectReference);
}

# Address of a worker or node manager.
struct Address {
  rayletId @0 : Data;
  ipAddress @1 : Text;
  port @2 : Int32;
  # Optional unique id for the worker.
  workerId @3 : Text;
}

# Function descriptor for Java.
struct JavaFunctionDescriptor {
  className @0 : Text;
  functionName @1 : Text;
  signature @2 : Text;
}

# Function descriptor for Python.
struct PythonFunctionDescriptor {
  moduleName @0 : Text;
  className @1 : Text;
  functionName @2 : Text;
  functionHash @3 : Text;
}

# Function descriptor for C/C++.
struct CppFunctionDescriptor {
  # Remote function name.
  functionName @0 : Text;
  caller @1 : Text;
  className @2 : Text;
}

struct FunctionDescriptor {
  union {
    javaFunctionDescriptor @0 : JavaFunctionDescriptor;
    pythonFunctionDescriptor @1 : PythonFunctionDescriptor;
    cppFunctionDescriptor @2 : CppFunctionDescriptor;
  }
}

# Actor concurrency group is used to define a concurrent
# unit to indicate how some methods are performed concurrently.
struct ConcurrencyGroup {
  # The name of the method concurrency group.
  name @0 : Text;
  # The maximum concurrency of this group.
  maxConcurrency @1 : Int32;
  # Function descriptors of the actor methods that will run in this concurrency group.
  functionDescriptors @2 : List(FunctionDescriptor);
}

struct ActorCreationTaskSpec {
  # ID of the actor that will be created by this task.
  actorId @0 : Data;
  # The max number of times this actor should be restarted.
  # If this number is 0 the actor won't be restarted.
  # If this number is -1 the actor will be restarted indefinitely.
  maxActorRestarts @1 : Int64;
  # The max number of times tasks submitted on this actor should be retried
  # if the actor fails and is restarted.
  # If this number is 0 the tasks won't be resubmitted.
  # If this number is -1 the tasks will be resubmitted indefinitely.
  maxTaskRetries @2 : Int64;
  # The dynamic options used in the worker command when starting a worker process for
  # an actor creation task. If the list isn't empty, the options will be used to replace
  # the placeholder string `RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER` in the worker command.
  dynamicWorkerOptions @3 : List(Text);
  # The max number of concurrent calls for default concurrency group of this actor.
  maxConcurrency @4 : Int32;
  # Whether the actor is persistent.
  isDetached @5 : Bool;
  # Globally-unique name of the actor. Should only be populated when is_detached is true.
  name @6 : Text;
  # The namespace of the actor. Should only be populated when is_detached is true.
  rayNamespace @7 : Text;
  # Whether the actor use async actor calls.
  isAsyncio @8 : Bool;
  # Field used for storing application-level extensions to the actor definition.
  extensionData @9 : Text;
  # Serialized bytes of the Handle to the actor that will be created by this task.
  serializedActorHandle @10 : Data;
  # The concurrency groups of this actor.
  concurrencyGroups @11 : List(ConcurrencyGroup);
  # Whether to enable out of order execution.
  executeOutOfOrder @12 : Bool;
  # The max number of pending actor calls.
  maxPendingCalls @13 : Int32;
}

# Task spec of an actor task.
struct ActorTaskSpec {
  # Actor ID of the actor that this task is executed on.
  actorId @0 : Data;
  # The dummy object ID of the actor creation task.
  actorCreationDummyObjectId @1 : Data;
  # Number of tasks that have been submitted to this actor so far.
  actorCounter @2 : UInt64;
}


struct JobConfig {
  enum ActorLifetime {
    kDetached @0;
    kNonDetached @1;
  }

  # The jvm options for java workers of the job.
  jvmOptions @0 : List(Text);
  # A list of directories or files (jar files or dynamic libraries) that specify the
  # search path for user code. This will be used as `CLASSPATH` in Java, and `PYTHONPATH`
  # in Python. In C++, libraries under these paths will be loaded by 'dlopen'.
  codeSearchPath @1 : List(Text);
  # Runtime environment to run the code
  runtimeEnvInfo @2 : RuntimeEnvInfo;
  # The job's namespace. Named `ray_namespace` to avoid confusions when invoked in c++.
  rayNamespace @3 : Text;
  # An opaque kv store for job related metadata.
  metadata @4 : Map(Text, Text);
  # The default lifetime of actors in this job.
  # If the lifetime of an actor is not specified explicitly at runtime, this
  # default value will be applied.
  defaultActorLifetime @5 : ActorLifetime;
  # System paths of the driver scripts. Python workers need to search
  # these paths to load modules.
  pyDriverSysPath @6 : List(Text);
}

struct TaskSpec {
  #Type of this task.
  type @0 : TaskType;
  # Name of this task.
  name @1 : Text;
  # Language of this task.
  language @2 : Language;
  # Function descriptor of this task uniquely describe the function to execute.
  functionDescriptor @3 : FunctionDescriptor;
  # ID of the job that this task belongs to.
  jobId @4 : Data;
  # Task ID of the task.
  taskId @5 : Data;
  # Task ID of the parent task.
  parentTaskId @6 : Data;
  # A count of the number of tasks submitted by the parent task before this one.
  parentCounter @7 : UInt64;
  # Task ID of the caller. This is the same as parent_task_id for non-actors.
  # This is the actor ID (embedded in a nil task ID) for actors.
  callerId @8 : Data;
  #/ Address of the caller.
  callerAddress @9 : Address;
  # Task arguments.
  args @10 : List(TaskArg);
  # Number of return objects.
  numReturns @11 : UInt64;
  # Quantities of the different resources required by this task.

  struct FloatEntry {
    key @0 : Text;
    value @1 : Float64;
  }

  requiredResources @12 : List(FloatEntry);
  # The resources required for placing this task on a node. If this is empty,
  # then the placement resources are equal to the required_resources.
  requiredPlacementResources @13 : List(FloatEntry);
  # Task specification for an actor creation task.
  # This field is only valid when `type == ACTOR_CREATION_TASK`.
  actorCreationTaskSpec @14 : ActorCreationTaskSpec;
  # Task specification for an actor task.
  # This field is only valid when `type == ACTOR_TASK`.
  actorTaskSpec @15 : ActorTaskSpec;
  # Number of times this task may be retried on worker failure.
  maxRetries @16 : Int32;
  # Whether or not to skip the execution of this task. When it's true,
  # the receiver will not execute the task. This field is used by async actors
  # to guarantee task submission order after restart.
  skipExecution @17 : Bool;
  # Breakpoint if this task should drop into the debugger when it starts executing
  # and "" if the task should not drop into the debugger.
  debuggerBreakpoint @18 : Data;
  # Runtime environment for this task.
  runtimeEnvInfo @19 : RuntimeEnvInfo;
  # The concurrency group name in which this task will be performed.
  concurrencyGroupName @20 : Text;
  # Whether application-level errors (exceptions) should be retried.
  retryExceptions @21 : Bool;
  # A serialized exception list that serves as an allowlist of frontend-language
  # exceptions/errors that should be retried.
  serializedRetryExceptionAllowlist @22 : Data;
  # The depth of the task. The driver has depth 0, anything it calls has depth
  # 1, etc.
  depth @23 : Int64;
  # Strategy about how to schedule this task.
  # scheduling_strategy @25 : SchedulingStrategy;
  schedulingStrategy @24 : Void;
  # A count of the number of times this task has been attempted so far. 0
  # means this is the first execution.
  attemptNumber @25 : UInt64;
  # This task returns a dynamic number of objects.
  returnsDynamic @26 : Bool;
  # A list of ObjectIDs that were created by this task but that should be
  # owned by the task's caller. The task should return the corresponding
  # ObjectRefs in its actual return value.
  # NOTE(swang): This should only be set when the attempt number > 0. On the
  # first execution, we do not yet know whether the task has dynamic return
  # objects.
  dynamicReturnIds @27 : List(Data);
  # Job config for the task. Only set for normal task or actor creation task.
  jobConfig @28 : JobConfig;
  # TODO(rickyx): Remove this once we figure out a way to handle task ids
  # across multiple threads properly.
  # The task id of the CoreWorker's main thread from which the task is submitted.
  # This will be the actor creation task's task id for concurrent actors. Or
  # the main thread's task id for other cases.
  submitterTaskId @29 : Data;
  # True if the task is a streaming generator. When it is true,
  # returns_dynamic has to be true as well. This is a temporary flag
  # until we migrate the generator implementatino to streaming.
  # TODO(sang): Remove it once migrating to the streaming generator
  # by default.
  streamingGenerator @30 : Bool;
  # Some timestamps of the task's lifetime, useful for metrics.
  # The time the task's dependencies have been resolved.
  dependencyResolutionTimestampMs @31 : Int64;
  # The time that the task is given a lease, and sent to the executing core
  # worker.
  leaseGrantTimestampMs @32 : Int64;
  # Number of return values from a completed streaming generator return.
  # The value is set only when a task is completed once.
  numStreamingGeneratorReturns @33 : Int64;
}
