# spark-gateway Helm Chart

## Manual Deploy
```shell
# Manual Deployments
helm upgrade \
  --install \
  --kube-context k8s-cluster \
  --namespace spark-gateway \
  spark-gateway .
```

## Development

### Debug
```shell
# See rendered k8s resources
helm template \
  --kube-context k8s-cluster \
  --namespace spark-gateway \
  spark-gateway .
```
