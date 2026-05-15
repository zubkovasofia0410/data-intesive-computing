#!/usr/bin/env python3

from pyspark import SparkContext
import json
import re
import sys
import heapq

# Constants
TOP_K = 75

TOKEN_SPLIT_RE = re.compile(
    r"[\s\d()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/]+",
    flags=re.UNICODE,
)

# Tokenization
def tokenize(text, stopwords):
    """
    Convert text into a set of unique normalized tokens.

    Steps:
    - lowercase
    - split using assignment delimiters
    - remove stopwords
    - remove single-character tokens

    Returns:
        set[str]
    """

    tokens = TOKEN_SPLIT_RE.split(text.lower())

    return {
        t for t in tokens
        if len(t) > 1 and t not in stopwords
    }

# Safe JSON parsing
def safe_parse(line):
    """
    Parse one JSON review safely.

    Returns:
        (category, reviewText)
        OR
        None
    """

    try:
        r = json.loads(line)

        category = r.get("category")
        text = r.get("reviewText") or ""

        if not category:
            return None

        return (category, text)

    except Exception:
        return None

def main():
    # Arguments
    input_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
    )

    output_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else "output_rdd.txt"
    )

    # Spark context
    sc = SparkContext(appName="ChiSquareRDD")

    # Load stopwords
    with open("stopwords.txt", "r", encoding="utf-8") as f:
        stopwords = set(
            w.strip().lower()
            for w in f
            if w.strip()
        )

    # Broadcast stopwords to workers
    bc_stopwords = sc.broadcast(stopwords)

    # Load reviews
    reviews = sc.textFile(input_path)

    # Parse JSON
    docs = (
        reviews
        .map(safe_parse)
        .filter(lambda x: x is not None)
    )

    # Tokenize
    tokenized = docs.map(
        lambda x: (
            x[0],
            tokenize(x[1], bc_stopwords.value)
        )
    )

    # Reused many times -> cache
    tokenized.cache()

    # Total number of documents
    N = tokenized.count()

    # Number of documents per category
    category_counts = (
        tokenized
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    category_count_map = category_counts.collectAsMap()

    # Broadcast metadata
    bc_category_counts = sc.broadcast(category_count_map)
    bc_N = sc.broadcast(N)

    # ---------------------------------------------------------
    # (term, category) -> A
    #
    # A = docs in category containing term
    # ---------------------------------------------------------

    term_category_counts = (
        tokenized
        .flatMap(
            lambda x: [
                ((term, x[0]), 1)
                for term in x[1]
            ]
        )
        .reduceByKey(lambda a, b: a + b)
    )

    # ---------------------------------------------------------
    # term -> total docs containing term
    # ---------------------------------------------------------

    term_total_counts = (
        tokenized
        .flatMap(
            lambda x: [
                (term, 1)
                for term in x[1]
            ]
        )
        .reduceByKey(lambda a, b: a + b)
    )

    term_total_map = term_total_counts.collectAsMap()

    # Broadcast term totals
    bc_term_totals = sc.broadcast(term_total_map)

    # Compute chi-square
    def compute_chi_square(record):

        (term, category), A = record

        total_with_term = bc_term_totals.value[term]

        n_c = bc_category_counts.value[category]

        N = bc_N.value

        B = total_with_term - A
        C = n_c - A
        D = N - n_c - B

        denominator = (
            (A + B) *
            (A + C) *
            (B + D) *
            (C + D)
        )

        if denominator <= 0:
            return None

        diff = A * D - B * C

        chi2 = (N * diff * diff) / denominator

        return (category, (term, chi2))

    chi_scores = (
        term_category_counts
        .map(compute_chi_square)
        .filter(lambda x: x is not None)
    )

    # Top 75 terms per category
    def top_k_terms(values):

        heap = []

        for term, chi2 in values:

            if len(heap) < TOP_K:
                heapq.heappush(heap, (chi2, term))

            elif chi2 > heap[0][0]:
                heapq.heapreplace(heap, (chi2, term))

        return sorted(
            heap,
            key=lambda x: (-x[0], x[1])
        )

    top_terms = (
        chi_scores
        .groupByKey()
        .mapValues(top_k_terms)
    )

    # Collect results
    results = top_terms.collect()

    # Sort categories alphabetically
    results.sort(key=lambda x: x[0])

    # Build merged dictionary
    merged_dictionary = set()

    for _, terms in results:
        for _, term in terms:
            merged_dictionary.add(term)

    merged_dictionary = sorted(merged_dictionary)

# Prepare output lines
output_lines = []

for category, terms in results:

    line = category

    for chi2, term in terms:
        line += f" {term}:{chi2:.4f}"

    output_lines.append(line)

# Last line = merged dictionary
output_lines.append(" ".join(merged_dictionary))

# Save output using Spark
sc.parallelize(output_lines, 1).saveAsTextFile(output_path)

if __name__ == "__main__":
    main()