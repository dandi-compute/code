#!/bin/bash

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH=""
if [ -n "$3" ]; then
    CONFIG_PATH="$3"
fi

# Create temporary job script
JOB_SCRIPT=$(mktemp $HOME/tmp/slurm_job.XXXXXX.sh)

cat > "$JOB_SCRIPT" <<'EOT'
#!/bin/bash
#SBATCH --job-name=AIND-Ephys-Pipeline
#SBATCH --mem=16GB
#SBATCH --partition=mit_normal
#SBATCH --time=4:00:00

# File has been modified from AIND docs for SLURM submission

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH="$3"

# Base dirs (define before using them)
BASE_DANDI_DIR="/orcd/data/dandi/001"
DANDI_COMPUTE_DIR="$BASE_DANDI_DIR/all-dandi-compute"
DANDI_ARCHIVE_DIR="$BASE_DANDI_DIR/s3dandiarchive"

if [ -z "$CONFIG_PATH" ]; then
    # Use default config
    CONFIG_FILE="$DANDI_COMPUTE_DIR/dandi-compute/pipeline-aind+ephys/default.config"
else
    CONFIG_FILE="$DANDI_COMPUTE_DIR/dandi-compute/pipeline-aind+ephys/blobs/$BLOB_ID/run-$RUN_ID/$CONFIG_PATH"
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file does not exist at $CONFIG_FILE"
    exit 1
fi

# Welcome message and input display
echo ""
echo "Deploying AIND Ephys Pipeline on MIT Engaging cluster"
echo "====================================================="
echo ""
echo "BLOB ID: $BLOB_ID"
echo "RUN ID: $RUN_ID"
echo "CONFIG FILE: $CONFIG_FILE"
echo ""

# modify this section to make the nextflow command available to your environment
source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/env_nf

PIPELINE_PATH="$DANDI_COMPUTE_DIR/aind-ephys-pipeline.source"
BASE_WORKDIR="$DANDI_COMPUTE_DIR/work"
RUN_WORKDIR="$BASE_WORKDIR/blobs/$BLOB_ID/run-$RUN_ID"
NXF_APPTAINER_CACHEDIR="$BASE_WORKDIR/apptainer_cache"

DATA_PATH="$DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_ID:3:6}/$BLOB_ID"
if [ ! -e "$DATA_PATH" ]; then
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

# Run nextflow
DATA_PATH="$DATA_PATH" RESULTS_PATH="$RESULTS_PATH" NXF_APPTAINER_CACHEDIR="$NXF_APPTAINER_CACHEDIR" nextflow \
    -C "$CONFIG_FILE" \
    -log "$RESULTS_PATH/nextflow/nextflow.log" \
    run "$PIPELINE_PATH/pipeline/main_multi_backend.nf" \
    --work-dir "$RUN_WORKDIR" \
    --job_dispatch_args "--input nwb" \
    --nwb_ecephys_args "--backend hdf5"

hostname

exit 0
EOT

# Submit the job and pass script args so $1/$2/$3 are populated in the job script
sbatch --output "/orcd/data/dandi/001/all-dandi-compute/logs/pipeline-aind+ephys_job-%j_blob_${BLOB_ID}_run_${RUN_ID}.log" "$JOB_SCRIPT" "$BLOB_ID" "$RUN_ID" "$CONFIG_PATH"

# Clean up
rm -f "$JOB_SCRIPT"