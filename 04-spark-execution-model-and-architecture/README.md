# Executing Spark Programs

**Table of Contents**

1. [Introduction](#introduction)
2. [Methods of Execution](#methods-of-execution)
   - [Interactive Client](#interactive-client)
   - [Job Submission](#job-submission)
3. [Spark Application Architecture (Driver and Executors)](#spark-application-architecture-driver-and-executors)
   - [Application Isolation Example](#application-isolation-example)
4. [Execution Environments (Cluster Managers)](#execution-environments-cluster-managers)
   - [Local Mode (`local[n]`)](#local-mode-localn)
5. [Deployment Modes: Client vs. Cluster](#deployment-modes-client-vs-cluster)
   - [Client Mode](#client-mode)
   - [Cluster Mode](#cluster-mode)
6. [Summary: Cluster Managers and Deployment Modes](#summary-cluster-managers-and-deployment-modes)

## Introduction

Executing Spark programs effectively requires understanding Spark's underlying architecture and execution model. It's not always as simple as running a standard script, especially in clustered environments.

## Methods of Execution

There are primarily two ways to run Spark code:

### Interactive Client

- **Tools:** `spark-shell` (Scala REPL), `pyspark` (Python REPL), notebooks (`Jupyter`, `Zeppelin`, `Databricks`, etc.).
- **Use Cases:** Best suited for learning, interactive data exploration, development, and debugging cycles.
- **How it works:** You interactively type and execute Spark commands or code cells.

### Job Submission

- **Tools:** `spark-submit` command-line utility, Databricks job scheduler, REST APIs (like Apache Livy), Cloud provider interfaces.
- **Use Cases:** Primarily used for running production applications, batch jobs, and automated workflows on a cluster.
- **Key Points:**
  - Allows packaging your application (e.g., Python files, JARs) and submitting it to a Spark cluster for execution.
  - `spark-submit` is the standard, universal command-line tool that works across most Spark environments (Standalone, YARN, Kubernetes, Mesos).
  - Other methods (Databricks notebooks as jobs, REST APIs) can be specific to certain platforms or vendors but often use `spark-submit` or similar mechanisms underneath.

## Spark Application Architecture (Driver and Executors)

- Spark utilizes a distributed architecture for each application it runs. When a Spark application starts, it creates a central coordinator process and multiple worker processes.
- **Driver:** The **Driver** process is the "master" or coordinator for a specific Spark application. It runs the `main()` function of your application (or manages the interactive session), creates the `SparkContext` (or `SparkSession`), analyzes and plans the execution of Spark jobs, and schedules tasks to be run on the executors.
- **Executors:** The **Executors** are "slave" or worker processes launched specifically for an application on worker nodes in the cluster. Executors are responsible for:
  - Executing the individual **tasks** assigned to them by the Driver.
  - Storing data partitions in memory or on disk (caching).
  - Reporting task status and results back to the Driver.
- **Cluster Manager:** The Driver and Executors run within containers or processes managed by an underlying **Cluster Manager** (e.g., Standalone Spark Master, YARN ResourceManager, Kubernetes API Server, Mesos). Spark relies on the Cluster Manager to allocate the necessary resources (CPU, memory) for the Driver and Executors.

> When an application is submitted, the Spark framework requests resources from the Cluster Manager. Typically, it first requests a container/process to launch the Driver. Once the Driver is running, it then requests additional containers/processes from the Cluster Manager to launch the Executors needed for the application. This resource negotiation happens for every Spark application submitted to the cluster.

<p align="center">
    <img src="https://github.com/user-attachments/assets/c5035beb-3445-469f-9c9b-8c29806ad007" width="75%" alt="Spark Application Components Diagram 1">
</p>
<p align="center">
    <img src="https://github.com/user-attachments/assets/22744b46-2198-432f-8fc7-29c58126834e" width="75%" alt="Spark Application Components Diagram 2">
</p>

### Application Isolation Example

Consider submitting two different Spark applications using `spark-submit`:

- **Application `A1` Submission:**
  - Spark requests a container from the Cluster Manager to start the Driver process for `A1`.
  - Once the `A1` Driver starts, it requests containers for its Executors.
  - The `A1` Driver and its dedicated set of Executors run the code for application `A1`.
- **Application `A2` Submission:**
  - Spark requests a _separate_ container from the Cluster Manager to start the Driver process for `A2`.
  - Once the `A2` Driver starts, it requests containers for _its own_ Executors.
  - The `A2` Driver and its dedicated set of Executors run the code for application `A2`.

> Each Spark application (`A1`, `A2`) runs independently on the cluster. They have their own dedicated Driver and Executor processes, ensuring isolation. This architecture allows multiple applications to run concurrently on the same cluster without interfering with each other, managed by the underlying Cluster Manager.

<p align="center">
    <img src="https://github.com/user-attachments/assets/8853c91d-0810-48ae-ba02-e9c032a9a369" width="75%" alt="Multiple Spark Applications Diagram">
</p>

## Execution Environments (Cluster Managers)

Spark is designed to run on various cluster managers, providing flexibility in deployment:

- **Local Mode (`local[n]`)**: Simulates Spark execution on a single machine.
- **Apache YARN**: A widely used resource manager in the Hadoop ecosystem.
- **Kubernetes (K8s)**: A popular container orchestration system.
- **Apache Mesos**: Another general-purpose cluster manager (less common for Spark now).
- **Standalone Mode**: A simple cluster manager included with Spark.

### Local Mode (`local[n]`)

- This mode is for running Spark applications on a **single machine**, often your laptop or development workstation. It does not require a separate cluster manager.
- `local`: Runs Spark with one thread (no parallelism).
- `local[n]`: Runs Spark using `n` threads (e.g., `local[4]` uses 4 threads).
- `local[*]`: Runs Spark using as many threads as there are logical CPU cores on the machine.
- In local mode, the Driver and Executors run as threads within a single JVM process, simulating a distributed environment locally. It's excellent for development, testing, and debugging.

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/b1759336-c22c-4c87-9a28-d3c15d09d28c" alt="Local Mode Single Thread">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/facb9862-7750-4f3d-94ae-c5bb8d60abae" alt="Local Mode Multiple Threads">
        </td>
</table>

## Deployment Modes: Client vs. Cluster

When submitting applications to a cluster manager (like YARN, Kubernetes, or Standalone), Spark offers two main deployment modes:

### Client Mode

- **Where Driver Runs:** The Spark Driver process runs on the **client machine** where the application was submitted (e.g., your laptop, an edge node).
- **Where Executors Run:** Executors are launched and run **inside the cluster** managed by the Cluster Manager.
- **Use Cases:** Suitable for interactive sessions (`spark-shell`, `pyspark`, notebooks connected to a cluster) where immediate feedback is desired. Also useful for debugging.
- **Drawbacks:**
  - The client machine must remain active for the duration of the application; if the client process terminates or loses network connectivity to the cluster, the application fails.
  - The Driver can become a network bottleneck as it communicates directly with all Executors from outside the cluster.
  - Less suitable for long-running production jobs.

> In client mode, the driver process runs locally on the client machine that initiated the connection. However, this driver still connects to the cluster manager to request and manage executor resources on the cluster nodes. If you quit the client session (e.g., close the `spark-shell` or notebook kernel), the driver process terminates, which in turn usually causes the cluster manager to kill the associated executors.

<p align="center">
    <img src="https://github.com/user-attachments/assets/1c013749-1172-4066-be86-6d22299f4fb4" width="75%" alt="Client Mode Diagram">
</p>

### Cluster Mode

- **Where Driver Runs:** The Spark Driver process runs **inside the cluster**, launched within a container or process managed by the Cluster Manager (e.g., in the YARN Application Master container or a dedicated Kubernetes pod).
- **Where Executors Run:** Executors are also launched and run **inside the cluster**, managed by the Cluster Manager.
- **Use Cases:** Preferred mode for production jobs and long-running applications submitted via `spark-submit`.
- **Advantages:**
  - The application lifecycle is managed entirely by the cluster, making it resilient to client disconnects. Once submitted, the job runs independently of the submitting machine.
  - Driver runs closer to the Executors within the cluster network, potentially reducing network latency.
- **Drawbacks:** Accessing Driver logs for debugging can sometimes require going through the Cluster Manager's UI or log aggregation system.

> In cluster mode, the entire application (Driver and Executors) runs within the cluster infrastructure. After submitting the job using tools like `spark-submit`, you can safely disconnect the client machine, and the application will continue to run until completion or failure. This makes cluster mode the standard choice for production deployments.

## Summary: Cluster Managers and Deployment Modes

- The first choice when running a Spark application (outside of local mode) is the **Cluster Manager**.
  - `local[n]`: Single machine, no cluster manager needed.
  - `YARN`: Common in Hadoop distributions (e.g., Cloudera) and some cloud services (e.g., older AWS EMR, Google Dataproc configurations).
  * `Kubernetes`: Increasingly popular for running Spark in containerized environments, both on-premise and in the cloud.
  * `Standalone`: Simple manager bundled with Spark, suitable for dedicated Spark clusters.
  * Cloud Platforms like `Databricks` often provide their own optimized runtime environments that manage Spark clusters transparently, often built on top of cloud VMs and sometimes abstracting YARN/Kubernetes details.
- When submitting to a cluster, the second choice is the **Deployment Mode**:
  - `client`: Driver runs on the submitting machine. Good for interactive work.
  * `cluster`: Driver runs within the cluster. Best for production jobs.

| Cluster Manager         | Deployment Mode     | Common Execution Tools / Use Case                     |
| :---------------------- | :------------------ | :---------------------------------------------------- |
| `local[n]`              | (N/A - Client-like) | IDE, Notebooks, Local Scripts                         |
| YARN / K8s / Standalone | `client`            | `spark-shell`, `pyspark`, Notebooks (Interactive/Dev) |
| YARN / K8s / Standalone | `cluster`           | `spark-submit` (Production Batch Jobs)                |
