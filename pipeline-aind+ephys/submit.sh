#!/bin/bash

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH=""
if [ -n "$3" ]; then
    CONFIG_PATH="$3"
fi

LOG_PATH="/orcd/data/dandi/001/all-dandi-compute/001675/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/logs/job-%j.log"
if [ -n "$(ls -A "$(dirname "$LOG_PATH")" 2>/dev/null)" ]; then
    echo "Error: Log directory is not empty at $(dirname "$LOG_PATH")"
    echo "Please use a different RUN ID or remove the existing directory."
    exit 1
fi
mkdir -p "$(dirname "$LOG_PATH")"

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
LOG_PATH="$4"

# Base dirs (define before using them)
BASE_DANDI_DIR="/orcd/data/dandi/001"
DANDI_ARCHIVE_DIR="$BASE_DANDI_DIR/s3dandiarchive"
DANDI_COMPUTE_BASE_DIR="$BASE_DANDI_DIR/all-dandi-compute"
DANDI_COMPUTE_GIT_DIR="$DANDI_COMPUTE_BASE_DIR/dandi-compute"
DANDISET_DIR="$DANDI_COMPUTE_BASE_DIR/001675"

# PIPELINE_PATH="$DANDI_COMPUTE_BASE_DIR/aind-ephys-pipeline.source"
PIPELINE_PATH="$DANDI_COMPUTE_BASE_DIR/aind-ephys-pipeline.cody"

BASE_WORKDIR="$DANDI_COMPUTE_BASE_DIR/work"
RUN_WORKDIR="$BASE_WORKDIR/blob-$BLOB_ID/run-$RUN_ID"
NXF_APPTAINER_CACHEDIR="$BASE_WORKDIR/apptainer_cache"

TRUE_DATA_PATH="$DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_ID:3:3}/$BLOB_ID"
SOURCE_DATA="$DANDISET_DIR/pipeline-aind+ephys/blob-$BLOB_ID/sourcedata"
SYMLINK_PATH="$SOURCE_DATA/$(basename "$BLOB_ID.nwb")"

RESULTS_PATH="$DANDISET_DIR/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/results"

if [ -z "$CONFIG_PATH" ]; then
    CONFIG_FILE="$DANDI_COMPUTE_GIT_DIR/pipeline-aind+ephys/default.config"
else
    CONFIG_FILE="$DANDI_COMPUTE_GIT_DIR/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/$CONFIG_PATH"
fi
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file does not exist at $CONFIG_FILE"
    exit 1
fi

if [ ! -e "$TRUE_DATA_PATH" ]; then
    echo "Error: Data file does not exist at $TRUE_DATA_PATH"
    echo "Please check the blob ID."
    exit 1
fi

if [ ! -e "$SYMLINK_PATH" ]; then
    mkdir -p "$(dirname "$SYMLINK_PATH")"
    ln -sf "$TRUE_DATA_PATH" "$SYMLINK_PATH"
fi

if [ -d "$RESULTS_PATH" ]; then
    echo "Error: Run directory already exists at $RESULTS_PATH"
    echo "Please use a different RUN ID or remove the existing directory."
    exit 1
fi

echo ""
echo "Deploying AIND Ephys Pipeline on MIT Engaging cluster"
echo "====================================================="
echo ""
echo "Blob ID: $BLOB_ID"
echo "Run ID: $RUN_ID"
echo "Config file: $CONFIG_FILE"
echo "Base work directory: $BASE_WORKDIR"
echo "Runner work directory: $RUN_WORKDIR"
echo "Apptainer cache: $NXF_APPTAINER_CACHEDIR"
echo "True data path: $TRUE_DATA_PATH"
echo "Source data: $SOURCE_DATA"
echo "Symlink path: $SYMLINK_PATH"
echo ""

source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/env_nf

DATA_PATH="$SOURCE_DATA" RESULTS_PATH="$RESULTS_PATH" NXF_APPTAINER_CACHEDIR="$NXF_APPTAINER_CACHEDIR" nextflow \
    -C "$CONFIG_FILE" \
    -log "$(dirname "$LOG_PATH")/nextflow.log" \
    run "$PIPELINE_PATH/pipeline/main_multi_backend.nf" \
    --work-dir "$RUN_WORKDIR" \
    --job_dispatch_args "--input nwb --nwb-files $TRUE_DATA_PATH" \
    --nwb_ecephys_args "--backend hdf5"

hostname

exit 0
EOT

sbatch --output "$LOG_PATH" "$JOB_SCRIPT" "$BLOB_ID" "$RUN_ID" "$CONFIG_PATH" "$LOG_PATH"
rm -f "$JOB_SCRIPT"
