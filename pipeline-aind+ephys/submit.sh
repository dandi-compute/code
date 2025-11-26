#!/bin/bash
sbatch <<EOT
#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8GB
#SBATCH --partition=mit_normal_gpu
#SBATCH --time=2:00:00
#SBATCH --output /orcd/data/dandi/001/test_aind/logs/aind-%j.log

# File has been modified from AIND docs for SLURM submission

# modify this section to make the nextflow command available to your environment
# e.g., using a conda environment with nextflow installed
source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/env_nf

DANDI_DIR="/orcd/data/dandi/001"
BASE_DIR="$DANDI_DIR/test_aind"

PIPELINE_PATH="$BASE_DIR/aind-ephys-pipeline.source"
DATA_PATH="$BASE_DIR/sample_data/data"
RESULTS_PATH="$BASE_DIR/results"
WORKDIR="$BASE_DIR/work"
NXF_APPTAINER_CACHEDIR="$WORKDIR/apptainer_cache"

CONFIG_FILE="$BASE_DIR/nextflow_slurm_custom.config"

DATA_PATH=$DATA_PATH RESULTS_PATH=$RESULTS_PATH NXF_APPTAINER_CACHEDIR=$NXF_APPTAINER_CACHEDIR nextflow \
    -C $CONFIG_FILE \
    -log $RESULTS_PATH/nextflow/nextflow.log \
    run $PIPELINE_PATH/pipeline/main_multi_backend.nf \
    --work-dir $WORKDIR \
    --job_dispatch_args "--input nwb" \
    --nwb_ecephys_args "--backend hdf5"
#    --params_file "$PIPELINE_PATH/.github/workflows/params_test.json"

hostname

exit 0
EOT
