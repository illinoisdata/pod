#!/bin/bash
#SBATCH --job-name="pod_job_exp1"
#SBATCH --output="pod_job_exp1.%j.%N.out"
#SBATCH --partition=gpuA40x4
#SBATCH --mem=62G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1  # could be 1 for py-torch
#SBATCH --cpus-per-task=16   # spread out to use 1 core per numa, set to 64 if tasks is 1
#SBATCH --constraint="scratch"
#SBATCH --gpus-per-node=1
#SBATCH --gpu-bind=closest   # select a cpu close to gpu on pci bus topology
#SBATCH --account=bdjx-delta-gpu    # <- match to a "Project" returned by the "accounts" command
#SBATCH --no-requeue
#SBATCH -t 18:00:00
#SBATCH -e slurm/slurm-%j.err
#SBATCH -o slurm/slurm-%j.out

# Assumptions:
# (1) Cloned repo at /projects/bdjx/${USER}/pod
# (2) Installed `pod` via `pip install -e .`

module reset # drop modules and explicitly load the ones needed
             # (good job metadata and reproducibility)
             # $WORK and $SCRATCH are now set
module load anaconda3_gpu
conda deactivate
conda deactivate
cd /projects/bdjx/${USER}/pod
source .venv/bin/activate
module list  # job documentation and metadata
echo "Job is starting on `hostname`"

export output=log_${USER}.txt; echo "@@@@@@@@@@@@" >> $output; (bash experiments/bench_exp1.sh shev,criu tseqpred,wordlang) 2>& 1 | tee -a $output; unset output
