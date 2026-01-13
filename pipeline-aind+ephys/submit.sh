#!/bin/bash

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH=""
if [ -n "$3" ]; then
    CONFIG_PATH="$3"
fi

LOG_PATH="/orcd/data/dandi/001/dandi-compute/001675/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/logs/job-%j.log"
if [ -n "$(ls -A "$(dirname "$LOG_PATH")" 2>/dev/null)" ]; then
    echo "Error: Log directory is not empty at $(dirname "$LOG_PATH")"
    echo "Please use a different RUN ID or remove the existing directory."
    exit 1
    # TODO: also confirm this remotely prior to running so we can do minimal `dandi download --dandiset.yml`
    # Also for results dir
fi
mkdir -p "$(dirname "$LOG_PATH")"

JOB_SCRIPT=$(mktemp $HOME/tmp/slurm_job.XXXXXX.sh)

cat > "$JOB_SCRIPT" <<'EOT'
#!/bin/bash
#SBATCH --job-name=AIND-Ephys-Pipeline
#SBATCH --mem=16GB
#SBATCH --partition=mit_normal
#SBATCH --time=4:00:00

BLOB_ID="$1"
RUN_ID="$2"
CONFIG_PATH="$3"
LOG_PATH="$4"

BLOB_HEAD="${BLOB_ID:0:1}"
case "$BLOB_HEAD" in
    [0-9])
        PARTITION="001"
        ;;
    [a-f])
        BLOB_HEAD_DECIMAL=$((16#$BLOB_HEAD))
        if [ "$BLOB_HEAD_DECIMAL" -ge 10 ]; then
            PARTITION="002"
        else
            PARTITION="001"
        fi
        ;;
    *)
        echo "Error: Invalid blob ID format"
        exit 1
        ;;
esac

BASE_DANDI_DIR="/orcd/data/dandi/001"
DANDI_COMPUTE_BASE_DIR="$BASE_DANDI_DIR/dandi-compute"
DANDI_COMPUTE_CODE="$DANDI_COMPUTE_BASE_DIR/code"
DANDISET_DIR="$DANDI_COMPUTE_BASE_DIR/001675"

# TODO: Currently need to run from Cody's modified branches until all PRs are merged
# PIPELINE_PATH="$DANDI_COMPUTE_BASE_DIR/aind-ephys-pipeline.source"
PIPELINE_PATH="$DANDI_COMPUTE_BASE_DIR/aind-ephys-pipeline.cody"

WORKDIR="$DANDI_COMPUTE_BASE_DIR/work"
NXF_APPTAINER_CACHEDIR="$WORKDIR/apptainer_cache"

DANDI_ARCHIVE_DIR="/orcd/data/dandi/$PARTITION/s3dandiarchive"
NWB_FILE_PATH="$DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_ID:3:3}/$BLOB_ID"
RESULTS_PATH="$DANDISET_DIR/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/results"
DATA_PATH="$(dirname "$NWB_FILE_PATH")"

if [ -z "$CONFIG_PATH" ]; then
    CONFIG_FILE="$DANDI_COMPUTE_CODE/pipeline-aind+ephys/default.config"
else
    CONFIG_FILE="$DANDI_COMPUTE_CODE/pipeline-aind+ephys/blob-$BLOB_ID/run-$RUN_ID/$CONFIG_PATH"
fi
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file does not exist at $CONFIG_FILE"
    exit 1
fi

if [ ! -e "$NWB_FILE_PATH" ]; then
    echo "Error: Data file does not exist at $NWB_FILE_PATH"
    echo "Please check the blob ID."
    exit 1
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
echo "dandi-compute checkout: $(git -C /orcd/data/dandi/001/dandi-compute/dandi-compute describe --tags --always)"
echo "Blob ID: $BLOB_ID"
echo "Run ID: $RUN_ID"
echo "Config file: $CONFIG_FILE"
echo "Base work directory: $WORKDIR"
echo "Apptainer cache: $NXF_APPTAINER_CACHEDIR"
echo "NWB file path: $NWB_FILE_PATH"
echo "DATA_PATH: $DATA_PATH"
echo "RESULTS_PATH: $RESULTS_PATH"
echo ""

source /etc/profile.d/modules.sh
module load miniforge
module load apptainer

conda activate /orcd/data/dandi/001/environments/name-nextflow_environment

DATA_PATH="$DATA_PATH" RESULTS_PATH="$RESULTS_PATH" NXF_APPTAINER_CACHEDIR="$NXF_APPTAINER_CACHEDIR" nextflow \
    -C "$CONFIG_FILE" \
    -log "$(dirname "$LOG_PATH")/nextflow.log" \
    run "$PIPELINE_PATH/pipeline/main_multi_backend.nf" \
    -work-dir "$WORKDIR" \
    --job_dispatch_args "--input nwb --nwb-files $NWB_FILE_PATH" \
    --nwb_ecephys_args "--backend hdf5"

hostname

exit 0
EOT

sbatch --output "$LOG_PATH" "$JOB_SCRIPT" "$BLOB_ID" "$RUN_ID" "$CONFIG_PATH" "$LOG_PATH"
rm -f "$JOB_SCRIPT"