.. _kuberay-gpu:

Using GPUs
==========
This document provides tips on GPU usage with KubeRay.

To use GPUs on Kubernetes, configure both your Kubernetes setup and add additional values to your Ray cluster configuration.

To learn about GPU usage on different clouds, see instructions for `GKE`_, for `EKS`_, and for `AKS`_.

Quickstart: Serve a GPU-based StableDiffusion model
___________________________________________________

You can find several GPU workload examples in the :ref:`examples <kuberay-examples>` section of the docs.
The :ref:`StableDiffusion example <kuberay-stable-diffusion-rayservice-example>` is a good place to start.

Dependencies for GPU-based machine learning
___________________________________________

The `Ray Docker Hub <https://hub.docker.com/r/rayproject/>`_ hosts CUDA-based container images packaged
with Ray and certain machine learning libraries.
For example, the image ``rayproject/ray-ml:2.6.3-gpu`` is ideal for running GPU-based ML workloads with Ray 2.6.3.
The Ray ML images are packaged with dependencies (such as TensorFlow and PyTorch) needed for the Ray Libraries that are used in these docs.
To add custom dependencies, use one, or both, of the following methods:

* Building a docker image using one of the official :ref:`Ray docker images <docker-images>` as base.
* Using :ref:`Ray Runtime environments <runtime-environments>`.


Configuring Ray pods for GPU usage
__________________________________

Using NVIDIA GPUs requires specifying `nvidia.com/gpu` resource `limits` and `requests` in the container fields of your `RayCluster`'s
`headGroupSpec` and/or `workerGroupSpecs`.

Here is a config snippet for a RayCluster workerGroup of up
to 5 GPU workers.

.. code-block:: yaml

   groupName: gpu-group
   replicas: 0
   minReplicas: 0
   maxReplicas: 5
   ...
   template:
       spec:
        ...
        containers:
         - name: ray-node
           image: rayproject/ray-ml:2.6.3-gpu
           ...
           resources:
            nvidia.com/gpu: 1 # Optional, included just for documentation.
            cpu: 3
            memory: 50Gi
           limits:
            nvidia.com/gpu: 1 # Required to use GPU.
            cpu: 3
            memory: 50Gi
            ...

Each of the Ray pods in the group can be scheduled on an AWS `p2.xlarge` instance (1 GPU, 4vCPU, 61Gi RAM).

.. tip::

    GPU instances are expensive -- consider setting up autoscaling for your GPU Ray workers,
    as demonstrated with the `minReplicas:0` and `maxReplicas:5` settings above.
    To enable autoscaling, remember also to set `enableInTreeAutoscaling:True` in your RayCluster's `spec`
    Finally, make sure you configured the group or pool of GPU Kubernetes nodes, to autoscale.
    Refer to your :ref:`cloud provider's documentation <kuberay-k8s-setup>` for details on autoscaling node pools.

GPU multi-tenancy
_________________

If a Pod doesn't include `nvidia.com/gpu` in its resource configurations, users typically expect the Pod to be unaware of any GPU devices, even if it's scheduled on a GPU node.
However, when `nvidia.com/gpu` isn't specified, the default value for `NVIDIA_VISIBLE_DEVICES` becomes `all`, giving the Pod awareness of all GPU devices on the node.
This behavior isn't unique to KubeRay, but is a known issue for NVIDIA.
A workaround is to set the `NVIDIA_VISIBLE_DEVICES` environment variable to `void` in the Pods which don't require GPU devices.

Some useful links:

- `NVIDIA/k8s-device-plugin#61`_
- `NVIDIA/k8s-device-plugin#87`_
- `[NVIDIA] Preventing unprivileged access to GPUs in Kubernetes`_
- `ray-project/ray#29753`_

GPUs and Ray
____________

This section discuss GPU usage for Ray applications running on Kubernetes.
For general guidance on GPU usage with Ray, see also :ref:`gpu-support`.

The KubeRay operator advertises container GPU resource limits to
the Ray scheduler and the Ray autoscaler. In particular, the Ray container's
`ray start` entrypoint will be automatically configured with the appropriate `--num-gpus` option.

GPU workload scheduling
~~~~~~~~~~~~~~~~~~~~~~~
After a Ray pod with access to GPU is deployed, it will
be able to execute tasks and actors annotated with gpu requests.
For example, the decorator `@ray.remote(num_gpus=1)` annotates a task or actor
requiring 1 GPU.


GPU autoscaling
~~~~~~~~~~~~~~~
The Ray autoscaler is aware of each Ray worker group's GPU capacity.
Say we have a RayCluster configured as in the config snippet above:

- There is a worker group of Ray pods with 1 unit of GPU capacity each.
- The Ray cluster does not currently have any workers from that group.
- `maxReplicas` for the group is at least 2.

Then the following Ray program will trigger upscaling of 2 GPU workers.

.. code-block:: python

    import ray

    ray.init()

    @ray.remote(num_gpus=1)
    class GPUActor:
        def say_hello(self):
            print("I live in a pod with GPU access.")

    # Request actor placement.
    gpu_actors = [GPUActor.remote() for _ in range(2)]
    # The following command will block until two Ray pods with GPU access are scaled
    # up and the actors are placed.
    ray.get([actor.say_hello.remote() for actor in gpu_actors])

After the program exits, the actors will be garbage collected.
The GPU worker pods will be scaled down after the idle timeout (60 seconds by default).
If the GPU worker pods were running on an autoscaling pool of Kubernetes nodes, the Kubernetes
nodes will be scaled down as well.

Requesting GPUs
~~~~~~~~~~~~~~~
You can also make a :ref:`direct request to the autoscaler <ref-autoscaler-sdk-request-resources>` to scale up GPU resources.

.. code-block:: python

    import ray

    ray.init()
    ray.autoscaler.sdk.request_resources(bundles=[{"GPU": 1}] * 2)

After the nodes are scaled up, they will persist until the request is explicitly overridden.
The following program will remove the resource request.

.. code-block:: python

    import ray

    ray.init()
    ray.autoscaler.sdk.request_resources(bundles=[])

The GPU workers can then scale down.

.. _kuberay-gpu-override:

Overriding Ray GPU capacity (advanced)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For specialized use-cases, it is possible to override the Ray pod GPU capacities advertised to Ray.
To do so, set a value for the `num-gpus` key of the head or worker group's `rayStartParams`.
For example,

.. code-block:: yaml

    rayStartParams:
        # Note that all rayStartParam values must be supplied as strings.
        num-gpus: "2"

The Ray scheduler and autoscaler will then account 2 units of GPU capacity for each
Ray pod in the group, even if the container limits do not indicate the presence of GPU.

GPU pod scheduling (advanced)
_____________________________

GPU taints and tolerations
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. note::

  Managed Kubernetes services typically take care of GPU-related taints and tolerations
  for you. If you are using a managed Kubernetes service, you might not need to worry
  about this section.

The `NVIDIA gpu plugin`_ for Kubernetes applies `taints`_ to GPU nodes; these taints prevent non-GPU pods from being scheduled on GPU nodes.
Managed Kubernetes services like GKE, EKS, and AKS automatically apply matching `tolerations`_
to pods requesting GPU resources. Tolerations are applied by means of Kubernetes's `ExtendedResourceToleration`_ `admission controller`_.
If this admission controller is not enabled for your Kubernetes cluster, you may need to manually add a GPU toleration to each of your GPU pod configurations. For example,

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
   generateName: example-cluster-ray-worker
   spec:
   ...
   tolerations:
   - effect: NoSchedule
     key: nvidia.com/gpu
     operator: Exists
   ...
   containers:
   - name: ray-node
     image: rayproject/ray:nightly-gpu
     ...

Node selectors and node labels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To ensure Ray pods are bound to Kubernetes nodes satisfying specific
conditions (such as the presence of GPU hardware), you may wish to use
the `nodeSelector` field of your `workerGroup`'s pod template `spec`.
See the `Kubernetes docs`_ for more about Pod-to-Node assignment.


Further reference and discussion
--------------------------------
Read about Kubernetes device plugins `here <https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/>`__,
about Kubernetes GPU plugins `here <https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus>`__,
and about NVIDIA's GPU plugin for Kubernetes `here <https://github.com/NVIDIA/k8s-device-plugin>`__.

.. _`GKE`: https://cloud.google.com/kubernetes-engine/docs/how-to/gpus
.. _`EKS`: https://docs.aws.amazon.com/eks/latest/userguide/eks-optimized-ami.html
.. _`AKS`: https://docs.microsoft.com/en-us/azure/aks/gpu-cluster

.. _`NVIDIA/k8s-device-plugin#61`: https://github.com/NVIDIA/k8s-device-plugin/issues/61
.. _`NVIDIA/k8s-device-plugin#87`: https://github.com/NVIDIA/k8s-device-plugin/issues/87
.. _`[NVIDIA] Preventing unprivileged access to GPUs in Kubernetes`: https://docs.google.com/document/d/1zy0key-EL6JH50MZgwg96RPYxxXXnVUdxLZwGiyqLd8/edit?usp=sharing
.. _`ray-project/ray#29753`: https://github.com/ray-project/ray/issues/29753

.. _`tolerations`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`taints`: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
.. _`NVIDIA gpu plugin`: https://github.com/NVIDIA/k8s-device-plugin
.. _`admission controller`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/
.. _`ExtendedResourceToleration`: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/#extendedresourcetoleration
.. _`Kubernetes docs`: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
.. _`bug`: https://github.com/ray-project/kuberay/pull/497/
