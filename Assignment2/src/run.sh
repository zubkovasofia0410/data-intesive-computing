set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SRC_DIR"

INPUT="${1:-hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json}"
OUTPUT_LOCAL="${2:-../output.txt}"

# Unique HDFS working dirs (so re-runs do not clash).
STAMP="$(date +%s)_$$"
COUNT_OUT="hdfs:///user/${USER:-$(whoami)}/dic_a1_${STAMP}/counts"
STATS_OUT="hdfs:///user/${USER:-$(whoami)}/dic_a1_${STAMP}/stats"
META_LOCAL="meta.tsv"

echo "[run.sh] Input        : $INPUT"
echo "[run.sh] Count output : $COUNT_OUT"
echo "[run.sh] Stats output : $STATS_OUT"

# ---------------------------------------------------------------------------
# stage 1
# ---------------------------------------------------------------------------
echo "[run.sh] Stage 1: chi_count.py"
python3 chi_count.py \
    -r hadoop \
    --file stopwords.txt \