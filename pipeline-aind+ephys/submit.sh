#!/bin/bash

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH=""
if [ -n "$3" ]; then
    CONFIG_PATH="$3"
fi

sbatch <<EOT
#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8GB
#SBATCH --partition=mit_normal
#SBATCH --time=2:00:00
#SBATCH --output /orcd/data/dandi/001/all-dandi-compute/logs/pipeline-aind+ephys_job-%j_blob-$1.log

# File has been modified from AIND docs for SLURM submission
echo "Deploying AIND Ephys Pipeline on MIT Engaging cluster"
echo "BLOB ID: $BLOB_ID"
echo "RUN ID: $RUN_ID"

# modify this section to make the nextflow command available to your environment
# e.g., using a conda environment with nextflow installed
source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/env_nf

DANDI_PARTITION_DIR="/orcd/data/dandi/001"
DANDI_COMPUTE_DIR="$DANDI_PARTITION_DIR/all-dandi-compute"
DANDI_ARCHIVE_DIR="$DANDI_PARTITION_DIR/s3dandiarchive"

PIPELINE_PATH="$DANDI_COMPUTE_DIR/aind-ephys-pipeline.source"
BASE_WORKDIR="$DANDI_COMPUTE_DIR/work"
RUN_WORKDIR="BASE_WORKDIR/blobs/$BLOB_ID/run-$RUN_ID"
NXF_APPTAINER_CACHEDIR="BASE_WORKDIR/apptainer_cache"

DATA_PATH="DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_IB:3:6}/$BLOB_ID"
if [ ! -f "$DATA_PATH" ]; then
    echo "Error: Data file does not exist at $DATA_PATH"
    echo "Please check the BLOB ID."
    exit 1
fi

RESULTS_PATH="$DANDI_COMPUTE_DIR/001675/pipeline-aind+ephys/results/blobs/$BLOB_ID/run-$RUN_ID/results"
if [ -d "$RESULTS_PATH" ]; then
    echo "Error: Run directory already exists at $RESULTS_PATH"
    echo "Please use a different RUN ID or remove the existing directory."
    exit 1
fi

if [ -z "$CONFIG_PATH" ]; then
    # Use default config
    CONFIG_FILE="$DANDI_COMPUTE_DIR/dandi-compute/pipeline-aind+ephys/default.config"
else
    CONFIG_FILE="$DANDI_COMPUTE_DIR/dandi-compute/pipeline-aind+ephys/blobs/$BLOD_ID/run-$RUN_ID/$CONFIG_PATH"
fi
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Config file does not exist at $CONFIG_PATH"
    exit 1
fi

# TODO: make params file specifiable

DATA_PATH=$DATA_PATH RESULTS_PATH=$RESULTS_PATH NXF_APPTAINER_CACHEDIR=$NXF_APPTAINER_CACHEDIR nextflow \
    -C $CONFIG_FILE \
    -log $RESULTS_PATH/nextflow/nextflow.log \
    run $PIPELINE_PATH/pipeline/main_multi_backend.nf \
    --work-dir $RUN_WORKDIR \
    --job_dispatch_args "--input nwb" \
    --nwb_ecephys_args "--backend hdf5"
#    --params_file "$PIPELINE_PATH/.github/workflows/params_test.json"

hostname

exit 0
EOT
