# Nova Sagsflyt — Caseworker Reassignment Pipeline

## Overview

This automation handles the transfer of cases and tasks in KMD Nova when a caseworker goes on temporary or permanent leave. It ensures that active work is properly reassigned, and that the new caseworker is made aware of their responsibilities.

---

## Process Description

1. **Fetch Case**
   The process retrieves the case using the provided *Sagsnummer* and verifies that it is currently owned by the **old caseworker**.

2. **Lookup New Caseworker**
   The new caseworker is identified by their RACF/AD identifier. To minimize unnecessary API calls, the process keeps an in-memory cache of lookups for the duration of a run. If the same new caseworker is requested again, the cached result is reused instead of performing another lookup.

3. **Reassign Active Tasks**
   All tasks on the case belonging to the old caseworker that are still active (not closed) are reassigned to the **new caseworker**.

4. **Reassign the Case**
   Ownership of the case itself is transferred from the old caseworker to the new one.

5. **Create Notification Task**
   A notification task is created on the case for the new caseworker. This task informs them that the case has been transferred and reminds them to review the case and reassign assistants on tasks if needed.

---

## Key Points

* **Active tasks only:** Closed tasks remain unchanged; only open ones are moved.
* **Caseworker cache:** Each new caseworker lookup is performed only once per run. Repeated references to the same RACF ID reuse cached data.
* **Notification:** The newly assigned caseworker always receives a task reminding them to review the case and delegate assistants if necessary.

---

## Purpose

This pipeline ensures continuity of case handling during staff changes, reduces manual reassignment work, and improves clarity for the receiving caseworker by explicitly notifying them of their new responsibilities.

# Robot-Framework V3

This repo is meant to be used as a template for robots made for [OpenOrchestrator](https://github.com/itk-dev-rpa/OpenOrchestrator).

## Quick start

1. To use this template simply use this repo as a template (see [Creating a repository from a template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template)).
__Don't__ include all branches.

2. Go to `robot_framework/__main__.py` and choose between the linear framework or queue based framework.

3. Implement all functions in the files:
    * `robot_framework/initialize.py`
    * `robot_framework/reset.py`
    * `robot_framework/process.py`

4. Change `config.py` to your needs.

5. Fill out the dependencies in the `pyproject.toml` file with all packages needed by the robot.

6. Feel free to add more files as needed. Remember that any additional python files must
be located in the folder `robot_framework` or a subfolder of it.

When the robot is run from OpenOrchestrator the `main.py` file is run which results
in the following:
1. The working directory is changed to where `main.py` is located.
2. A virtual environment is automatically setup with the required packages.
3. The framework is called passing on all arguments needed by [OpenOrchestrator](https://github.com/itk-dev-rpa/OpenOrchestrator).

## Requirements
Minimum python version 3.10

## Flow

This framework contains two different flows: A linear and a queue based.
You should only ever use one at a time. You choose which one by going into `robot_framework/__main__.py`
and uncommenting the framework you want. They are both disabled by default and an error will be
raised to remind you if you don't choose.

### Linear Flow

The linear framework is used when a robot is just going from A to Z without fetching jobs from an
OpenOrchestrator queue.
The flow of the linear framework is sketched up in the following illustration:

![Linear Flow diagram](Robot-Framework.svg)

### Queue Flow

The queue framework is used when the robot is doing multiple bite-sized tasks defined in an
OpenOrchestrator queue.
The flow of the queue framework is sketched up in the following illustration:

![Queue Flow diagram](Robot-Queue-Framework.svg)

## Linting and Github Actions

This template is also setup with flake8 and pylint linting in Github Actions.
This workflow will trigger whenever you push your code to Github.
The workflow is defined under `.github/workflows/Linting.yml`.

