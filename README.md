# Cluster Scheduling

---

We are currently working on cluster data analysis.

Based on [Graphene: Packing and Dependency-aware Scheduling  for Data-Parallel Clusters](https://urldefense.proofpoint.com/v2/url?u=https-3A__www.usenix.org_system_files_conference_osdi16_osdi16-2Dgrandl-2Dgraphene.pdf&d=DwMFaQ&c=009klHSCxuh5AI1vNQzSO0KGjl4nbi2Q0M1QLJX9BeE&r=DEq8DIQPbwANBsyzyzxSQv3mjmXjRODgIYtBTK-gui4&m=078MPcaTX48wul9O9gknhVcO3fsQTA6Ov6JI1in-ecXtU4icJBMG1SmTyloZeqfV&s=-jAA4VvdLT29JG8rZWsfp0NVKuHJ1t9X_nQnkGrCBs0&e=), we calculated the `CPLen` and `TWork` for [Google Borg cluster workload traces v3](https://github.com/google/cluster-data).   `ModCP` and `NewLB` will be completed soon.

## How to run it

### Requirements

For `CPLen_TWork`

- numpy
- pandas

For `Google_clusterdata_analysis_colab.ipynb`, please run in Google Colab environment.

As the cluster data is relatively big, you cannot download all of them, therefore a sample is provided in the repo.

- Make sure `instance_usage_SAMPLE.csv` and `time_duration_SAMPLE.csv` is in the same folder as `CPLen_TWork.py`. 

- Then run `CPLen_TWork.py` file.

  ```cmd
  python CPLen_TWork.py
  ```

## Google Colab Notebook

We also have a Jupyter Notebook `Google_clusterdata_analysis_colab.ipynb`, however you may not able to run it as you need a project-id to gain the access for the google sever.

