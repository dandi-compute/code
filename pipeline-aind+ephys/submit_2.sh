#!/bin/bash

TOKEN="$1"
DANDISET_ID="$2"
PATH_IN_DANDISET="$3"
CONFIG_PATH=""
if [ -n "$4" ]; then
    CONFIG_PATH="$4"
fi

# Create temporary job script
JOB_SCRIPT=$(mktemp $HOME/tmp/slurm_job.XXXXXX.sh)

cat > "$JOB_SCRIPT" <<'EOT'
#!/bin/bash
#SBATCH --job-name=pipeline-aind+ephys
#SBATCH --mem=16GB
#SBATCH --partition=mit_normal
#SBATCH --time=4:00:00

# File has been modified from AIND docs for SLURM submission

source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/env_nf

TOKEN="$1"
DANDISET_ID="$2"
PATH_IN_DANDISET="$3"
BLOB_ID="$(python get_blob_id.py $TOKEN $DANDISET_ID $PATH_IN_DANDISET)"
RUN_ID="$(python -c 'import uuid; print(str(uuid.uuid4())[:8])')"
CONFIG_PATH="$4"

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
echo "Blob ID: $BLOB_ID"
echo "Run ID: $RUN_ID"
echo "Config file: $CONFIG_FILE"

PIPELINE_PATH="$DANDI_COMPUTE_DIR/aind-ephys-pipeline.source"

BASE_WORKDIR="$DANDI_COMPUTE_DIR/work"
RUN_WORKDIR="$BASE_WORKDIR/blob-$BLOB_ID/run-$RUN_ID"
mkdir -p "$RUN_WORKDIR"

NXF_APPTAINER_CACHEDIR="$BASE_WORKDIR/apptainer_cache"

TRUE_DATA_PATH="$DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_ID:3:3}/$BLOB_ID"
if [ ! -e "$TRUE_DATA_PATH" ]; then
    echo "Error: Data file does not exist at $TRUE_DATA_PATH"
    echo "Please check the blob ID."
    exit 1
fi
echo "True data path: $TRUE_DATA_PATH"

SOURCE_DATA="$DANDI_COMPUTE_DIR/001675/pipeline-aind+ephys/blobs/$BLOB_ID/derived/sourcedata"
SYMLINK_PATH="$SOURCE_DATA/$(basename "$PATH_IN_DANDISET")"
if [ ! -e "$SYMLINK_PATH" ]; then
    mkdir -p "$(dirname "$SYMLINK_PATH")"
    ln -sf "$TRUE_DATA_PATH" "$SYMLINK_PATH"
fi

echo "Symlinked source data: $SOURCE_DATA"
echo ""

RESULTS_PATH="$DANDI_COMPUTE_DIR/001675/pipeline-aind+ephys/blobs/$BLOB_ID/run-$RUN_ID/results"
if [ -d "$RESULTS_PATH" ]; then
    echo "Error: Run directory already exists at $RESULTS_PATH"
    echo "Please use a different RUN ID or remove the existing directory."
    exit 1
fi

# Run nextflow
DATA_PATH="$SOURCE_DATA" RESULTS_PATH="$RESULTS_PATH" NXF_APPTAINER_CACHEDIR="$NXF_APPTAINER_CACHEDIR" nextflow \
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
sbatch --output "/orcd/data/dandi/001/all-dandi-compute/logs/pipeline-aind+ephys_job-%j.log" "$JOB_SCRIPT" "$TOKEN" "$DANDISET_ID" "$PATH_IN_DANDISET" "$CONFIG_PATH"

# Clean up
rm -f "$JOB_SCRIPT"