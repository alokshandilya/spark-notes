# How to execute spark programs

Executing spark programs is not simple and straight forward. One must have knowledge of spark architecture and execution model.

#### 2 methods:

- **Interactive client** (spark-shell, notebooks)
  - learning, development, exploration
- **Submit a job** (spark-submit, databricks notebook, REST API)
  - production
  - allow to submit spark jobs to the cluster
  - spark-submit is universal way and works in almost all environments
  - other methods are vendor specific

## Spark distributed processing model

- spark applies master slave architecture to every application.
- when we submit a application, it creates a master process, this master process creates a bunch of slaves to distribute the work. we can think of slaves as runtime containers with some dedicated CPU and memory.
- in spark terminology, master is called driver and slaves are called executors.
- it's about the application and the containers and not the cluster itself, the cluster itself might have a master node and multiple slave nodes. but these are part of cluster which is managed by the cluster manager.

> the spark engine is going to ask for a container from the underlying cluster manager to start the driver process. Once it's started, the driver is again going to ask for some more containers to start the executor processes. This happens for each application.

<p align="center">
    <img src="https://github.com/user-attachments/assets/c5035beb-3445-469f-9c9b-8c29806ad007" width="75%">
</p>
<p align="center">
    <img src="https://github.com/user-attachments/assets/22744b46-2198-432f-8fc7-29c58126834e" width="75%">
</p>

##### Example

- Let's say we are using spark-submit to submit a spark application `A1`.
  - spark engine will request cluster manager for a container to start the driver process for A1.
  - once the driver is started, driver will request more containers to start the executor processes.
  - these executors and driver are responsible for running you application code and doing the job.
- Let's say we are using spark-submit to submit another spark application `A2`.
  - spark engine will request cluster manager for a container to start the driver process for A2.
  - once the driver is started, driver will request more containers to start the executor processes.
  - these executors and driver are responsible for running you application code and doing the job.

> Here, `A1` and `A2` are 2 independent applications. Both following master-slave architecture and have their own dedicated driver and executors. Every spark application applies master-slave architecture and runs independly on the cluster.

<p align="center">
    <img src="https://github.com/user-attachments/assets/8853c91d-0810-48ae-ba02-e9c032a9a369" width="75%">
</p>

## How Spark executes a program on local machine

- spark engine is compatible with multiple clusters
  - `local[n]`
  - `YARN`
  - `Kubernetes`
  - `Mesos`
  - `Standalone`

#### `local[n]`

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/b1759336-c22c-4c87-9a28-d3c15d09d28c">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/facb9862-7750-4f3d-94ae-c5bb8d60abae">
        </td>
</table>

#### on an interactive tool

To run application, spark gives 2 choices:

- **client mode**: for interactive clients eg. spark-shell, notebooks

  > spark driver process runs locally on the client machine. However, driver still connects to the cluster manager and starts all the executors on the cluster.

  > if we quit the client or log off from client machine, then the driver dies and the executors also dies.

  > client mode is suitable for interactive work but not for long running jobs.

  <img src="https://github.com/user-attachments/assets/1c013749-1172-4066-be86-6d22299f4fb4" width="75%">

- **cluster**: for submitting jobs to the cluster eg. spark-submit

  > submit the application to the cluster and let it run. Everything runs on the cluster be it drivers or executors.

  > once the job is submitted, we can log off from the client machine and the job will still run.

  > cluster mode is suitable for production jobs, long running jobs.
