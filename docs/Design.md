# Design

## Architecture
**Spark-Gateway** has two components **Gateway**, responsible for routing, auth, etc, and **SparkManager**, responsible to 
managing SparkApplication resources on Kubernetes clusters. There is a SparkManager component per configured Kubernetes
cluster to allow for large horizontal scalability.

## Spark Application Lifecycle

### Spark Application CREATE Request Flow
Upon POST request to Spark-Gateway for submission, following steps will occur:
1. Gateway will receive the request and authenticate/authorize the User.
2. Gateway SparkApplication spec will be verified.
3. Gateway will add some new labels to the SparkApplication.
4. Gateway will determine the cluster from list of Kubernetes cluster to submit the SparkApplication to.
5. Gateway will create a POST request with SparkApplication to the Kubernetes cluster specific SparkManager.
6. SparkManager will create the SparkApplication resources on the Kubernetes cluster via the cluster's kube-apiserver.
7. SparkManager will verify that the SparkApplication resource exists on the cluster.
8. SparkManager will respond to Gateway's request with the SparkApplication Spec.
9. Gateway will persist the SparkApplication submission request to Database.
10. Gateway will respond to the client with the SparkApplication and some other information.

## SparkApplication naming
Gateway will generate a unique name for each SparkApp submission in `<clusterid>-<namespaceid>-<UUID>` format. The
`metadata.name` field will be replaced by the generated SparkApp name, and the original name will be added to the 
SparkApplication as an `applicationName` annotation.

## Code Architecture
Both Gateway and SparkManager are REST APIs that use [Gin Web Framework](https://github.com/gin-gonic/gin). Both follow 
the [**Handler-Service-Repository**](https://tom-collings.medium.com/controller-service-repository-16e29a4684e5) design
pattern commonly used to separate concerns and organize code in a clean and maintainable way. It splits the logic into 
three distinct layers:

#### Handler
The Handler is responsible for processing the incoming requests. It performs authN/Z, and request validation and then 
delegates tasks to the [Service layer](#service).

#### Service
The Service layer is where the business logic resides. It acts as an intermediary between the Handler and the Repository.
The Service typically contains the business rules that the application needs to perform. The Service might coordinate
multiple operations and is responsible for calling the appropriate methods in the [Repository layer](#Repository).

#### Repository
The Repository is the data access layer. It is responsible for interacting with the database or any external data source
to retrieve, store, update, or delete data. The Repository abstracts the underlying data access logic from the rest of 
the application, so the Service layer doesn't need to be concerned with how data is persisted or fetched.
