# Minikube Setup

### 1. Download and Install Minikube
Minikube docs: https://minikube.sigs.k8s.io/docs/start/

### 2. Start minikube
```bash
minikube start --driver=docker
```
See [minikube docs](https://minikube.sigs.k8s.io/docs/drivers/docker/) for more details.

### 3. Enable Pushing directly to the in-cluster Docker daemon (docker-env)

```bash
eval $(minikube docker-env)

docker ps # you should see the containers inside the minikube
```

> Note: Evaluating the docker-env is only valid for the current terminal. By closing the terminal, you will go back to using your own system’s docker daemon.
> In container-based drivers such as Docker or Podman, you will need to re-do docker-env each time you restart your minikube cluster.

See [minikube docs](https://minikube.sigs.k8s.io/docs/handbook/pushing/#6-pushing-directly-to-in-cluster-containerd-buildkitd) for more details.
