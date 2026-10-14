#!/usr/bin/env python3
"""Infer stable in-flight pgbench statements from PostgreSQL samples and native logs.

Wait events are observations at successful samples only. Sample errors and gaps are
reported; unchanged backend/query/transaction identity plus no native completion
supports continuity of the same in-flight statements across those gaps.
"""
import datetime as dt
import math


OBSERVED_WAIT_GAP_SECONDS = 2.5
DIRECT_TIMESTAMP_TOLERANCE_SECONDS = 0.000002
# Current pilot's matched consecutive active-query ages reconstructed starts
# varied by at most 2.8 ms; retain a 10 ms explicit allowance for measurement jitter.
RECONSTRUCTED_TIMESTAMP_TOLERANCE_SECONDS = 0.01


def _epoch(value, label):
    if isinstance(value, bool):
        raise ValueError(f"{label} is not a timestamp")
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        parsed = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{label} must include a timezone")
        result = parsed.timestamp()
    else:
        raise ValueError(f"{label} is not a timestamp")
    if not math.isfinite(result):
        raise ValueError(f"{label} is not finite")
    return result


def _utc(value):
    return dt.datetime.fromtimestamp(float(value), dt.timezone.utc).isoformat(
        timespec="microseconds").replace("+00:00", "Z")


def _finite_age(value, label):
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{label} is missing")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{label} is invalid")
    return number


def _observation(row, index, reconstructed_tolerance):
    if row.get("sampleStatus") != "ok":
        raise ValueError("sample is not successful")
    observed = _epoch(row.get("observedAtUtc"), f"sample {index} observedAtUtc")
    backends = row.get("backends")
    if not isinstance(backends, list) or len(backends) != 10:
        raise ValueError("sample does not contain ten writer backends")
    if row.get("benchSessions", 10) != 10:
        raise ValueError("sample does not confirm ten pgbench sessions")

    identities = {}
    waits = {}
    direct_seen = []
    for backend in backends:
        pid = backend.get("pid")
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 or pid in identities:
            raise ValueError("backend PIDs must be ten unique positive integers")
        if backend.get("state") != "active":
            raise ValueError(f"backend {pid} is not active")
        wait_event = backend.get("waitEvent")
        if not wait_event or wait_event == "ClientRead":
            raise ValueError(f"backend {pid} is not observed waiting in PostgreSQL")

        direct = "queryStartUtc" in backend or "xactStartUtc" in backend
        if direct:
            query_start = _epoch(backend.get("queryStartUtc"), f"backend {pid} queryStartUtc")
            xact_start = _epoch(backend.get("xactStartUtc"), f"backend {pid} xactStartUtc")
            backend_start_value = backend.get("backendStartUtc")
            backend_start = (_epoch(backend_start_value, f"backend {pid} backendStartUtc")
                             if backend_start_value is not None else None)
            direct_seen.append(True)
        else:
            query_start = observed - _finite_age(backend.get("queryAgeSeconds"),
                                                 f"backend {pid} queryAgeSeconds")
            xact_start = observed - _finite_age(backend.get("xactAgeSeconds"),
                                                f"backend {pid} xactAgeSeconds")
            backend_start = None
            direct_seen.append(False)
        if query_start > observed + reconstructed_tolerance or xact_start > observed + reconstructed_tolerance:
            raise ValueError(f"backend {pid} start time is after its observation")
        identities[pid] = {"pid": pid, "backendStartEpoch": backend_start,
                           "queryStartEpoch": query_start, "xactStartEpoch": xact_start,
                           "directTimestamps": direct}
        waits[pid] = {"waitEventType": backend.get("waitEventType"),
                      "waitEvent": wait_event}
    if len(set(direct_seen)) != 1:
        raise ValueError("sample mixes direct and age-reconstructed start timestamps")
    return {"epoch": observed, "identities": identities, "waits": waits,
            "timestampMethod": "direct" if direct_seen[0] else "reconstructed_from_age",
            "sourceIndex": index}


def _same_identity(anchor, current, tolerance):
    if anchor["timestampMethod"] != current["timestampMethod"]:
        return False
    if set(anchor["identities"]) != set(current["identities"]):
        return False
    effective_tolerance = (DIRECT_TIMESTAMP_TOLERANCE_SECONDS
                           if anchor["timestampMethod"] == "direct" else tolerance)
    for pid, before in anchor["identities"].items():
        after = current["identities"][pid]
        for field in ("backendStartEpoch", "queryStartEpoch", "xactStartEpoch"):
            a, b = before[field], after[field]
            if a is None or b is None:
                if a is not b:
                    return False
            elif abs(a-b) > effective_tolerance:
                return False
    return True


def _completion_epochs(transactions):
    result = []
    for index, transaction in enumerate(transactions):
        if isinstance(transaction, dict):
            value = transaction.get("completedAtEpoch", transaction.get("completedAtUtc"))
        else:
            value = transaction
        result.append(_epoch(value, f"native completion {index}"))
    return sorted(result)


def _sample_errors(samples, parsed_samples):
    offsets = []
    for row in parsed_samples:
        controller = row.get("sampleEpoch")
        try:
            controller_epoch = _epoch(controller, "sampleEpoch")
        except (ValueError, TypeError):
            continue
        offsets.append((controller_epoch, row["_parsed"]["epoch"]-controller_epoch))
    errors = []
    for index, row in enumerate(samples, 1):
        if row.get("sampleStatus") == "ok":
            continue
        try:
            controller_epoch = _epoch(row.get("sampleEpoch"), f"failed sample {index} sampleEpoch")
        except (ValueError, TypeError):
            controller_epoch = None
        mapped = None
        if controller_epoch is not None and offsets:
            nearest = min(offsets, key=lambda pair: abs(pair[0]-controller_epoch))
            mapped = controller_epoch+nearest[1]
        errors.append({"sourceIndex": index, "sampleEpoch": controller_epoch,
                       "sampleTimeUtc": _utc(controller_epoch) if controller_epoch is not None else None,
                       "mappedDbEpoch": mapped, "mappedDbTimeUtc": _utc(mapped) if mapped is not None else None,
                       "error": str(row.get("error", "sampleStatus is not ok"))})
    return errors


def _candidate(anchor, first, last, sample_count, gaps, error_details, completions):
    start, end = first["epoch"], last["epoch"]
    times = [t for t in completions if start <= t <= end]
    writers = []
    for pid in sorted(anchor["identities"]):
        identity = anchor["identities"][pid]
        writers.append({"pid": pid,
                        "backendStartUtc": _utc(identity["backendStartEpoch"])
                            if identity["backendStartEpoch"] is not None else None,
                        "queryStartUtc": _utc(identity["queryStartEpoch"]),
                        "xactStartUtc": _utc(identity["xactStartEpoch"])})
    wait_counts = {}
    for row in gaps.get("_waitObservations", []):
        for wait in row.values():
            key = f"{wait.get('waitEventType') or 'Unknown'}/{wait.get('waitEvent') or 'Unknown'}"
            wait_counts[key] = wait_counts.get(key, 0)+1
    segment_errors = [item for item in error_details
                      if item.get("mappedDbEpoch") is not None and start <= item["mappedDbEpoch"] <= end]
    gap_rows = [item for item in gaps.get("_gaps", [])
                if item["startEpoch"] >= start and item["endEpoch"] <= end]
    maximum_gap = gaps.get("_maxSuccessfulSampleGapSeconds", 0.0)
    return {"startUtc": _utc(start), "endUtc": _utc(end), "startEpoch": start, "endEpoch": end,
            "seconds": max(0.0, end-start), "writerCount": len(writers), "writers": writers,
            "successfulSamples": sample_count, "timestampMethod": anchor["timestampMethod"],
            "reconstructedStartToleranceSeconds": gaps["_tolerance"] if anchor["timestampMethod"] == "reconstructed_from_age" else None,
            "nativeCompletionCountWithin": len(times),
            "allWritersActiveAndWaitingAtSuccessfulSamples": True,
            "observedWaitEventCounts": dict(sorted(wait_counts.items())),
            "sampleErrorsInSpan": segment_errors,
            "unobservedSamplingGaps": gap_rows,
            "maximumSuccessfulSampleGapSeconds": maximum_gap,
            "listedGapPolicy": f"Lists successful-sample intervals exceeding {OBSERVED_WAIT_GAP_SECONDS:g} seconds or containing a mapped sample error; maximumSuccessfulSampleGapSeconds summarizes every adjacent successful observation.",
            "waitStateInterpretation": "Wait events are point observations at successful samples; no wait-event state is inferred inside unobserved gaps."}


def analyze_stable_writers(samples, transactions, low, high, *,
                           native_log_clean, native_counts_reconciled,
                           minimum_seconds=7200.0,
                           reconstructed_start_tolerance_seconds=RECONSTRUCTED_TIMESTAMP_TOLERANCE_SECONDS,
                           observed_wait_gap_seconds=OBSERVED_WAIT_GAP_SECONDS):
    """Return the longest interval with ten stable in-flight writers and no completions.

    Native log cleanliness and count reconciliation are explicit required inputs.
    Short or long sampler gaps do not themselves break a stable statement identity;
    every such gap and failed sample is retained in the result.
    """
    low_epoch, high_epoch = _epoch(low, "window start"), _epoch(high, "window end")
    if high_epoch < low_epoch:
        raise ValueError("window end precedes window start")
    tolerance = float(reconstructed_start_tolerance_seconds)
    if not math.isfinite(tolerance) or tolerance < 0 or tolerance > 0.5:
        raise ValueError("reconstructed start tolerance must be between 0 and 0.5 seconds")
    wait_gap_limit = float(observed_wait_gap_seconds)
    if not math.isfinite(wait_gap_limit) or wait_gap_limit <= 0:
        raise ValueError("observed wait gap limit must be positive")

    parsed = []
    invalid_successful_samples = []
    for index, row in enumerate(samples, 1):
        if row.get("sampleStatus") != "ok":
            continue
        try:
            value = _observation(row, index, tolerance)
            value["_row"] = row
            parsed.append(value)
        except (ValueError, TypeError, OverflowError) as exc:
            try:
                invalid_epoch = _epoch(row.get("observedAtUtc"), f"sample {index} observedAtUtc")
            except (ValueError, TypeError, OverflowError):
                invalid_epoch = None
            invalid_successful_samples.append({"sourceIndex": index, "observedAtUtc": row.get("observedAtUtc"),
                                               "observedEpoch": invalid_epoch, "reason": str(exc)})
    parsed.sort(key=lambda item: item["epoch"])
    all_successful = []
    for index, row in enumerate(samples, 1):
        if row.get("sampleStatus") != "ok":
            continue
        try:
            all_successful.append({"sampleEpoch": row.get("sampleEpoch"),
                                   "_parsed": {"epoch": _epoch(row.get("observedAtUtc"), f"sample {index} observedAtUtc")}})
        except (ValueError, TypeError):
            continue
    error_details = _sample_errors(samples, all_successful)
    completions = _completion_epochs(transactions)
    in_window = [row for row in parsed if low_epoch <= row["epoch"] <= high_epoch]
    invalid_epochs = sorted(item["observedEpoch"] for item in invalid_successful_samples
                            if item["observedEpoch"] is not None and
                            low_epoch <= item["observedEpoch"] <= high_epoch)
    gaps = []
    for before, after in zip(in_window, in_window[1:]):
        start, end = before["epoch"], after["epoch"]
        errors_between = [item for item in error_details if item.get("mappedDbEpoch") is not None
                          and start < item["mappedDbEpoch"] <= end]
        seconds = end-start
        if seconds > wait_gap_limit or errors_between:
            gaps.append({"startUtc": _utc(start), "endUtc": _utc(end), "startEpoch": start,
                         "endEpoch": end, "seconds": seconds,
                         "kind": "sample error between successful observations" if errors_between
                             else "successful-sample gap exceeds observed-wait limit",
                         "sampleErrorCount": len(errors_between)})

    best = None
    anchor = first = previous = None
    sample_count = 0
    segment_gaps = []
    segment_waits = []
    segment_max_gap = 0.0

    def finish():
        nonlocal best
        if anchor is None or first is None or previous is None:
            return
        candidate_gaps = {"_gaps": list(segment_gaps), "_waitObservations": list(segment_waits),
                          "_tolerance": tolerance,
                          "_maxSuccessfulSampleGapSeconds": segment_max_gap}
        candidate = _candidate(anchor, first, previous, sample_count, candidate_gaps,
                               error_details, completions)
        if best is None or candidate["seconds"] > best["seconds"]:
            best = candidate

    for row in in_window:
        if not row["identities"]:
            finish()
            anchor = first = previous = None
            sample_count = 0
            segment_gaps = []
            segment_waits = []
            segment_max_gap = 0.0
            continue
        same = anchor is not None and _same_identity(anchor, row, tolerance)
        completion_between = (previous is not None and
                              any(previous["epoch"] < value <= row["epoch"] for value in completions))
        if not same or completion_between:
            finish()
            anchor = row
            first = row
            previous = row
            sample_count = 1
            segment_gaps = []
            segment_waits = [row["waits"]]
            segment_max_gap = 0.0
            continue
        if previous is None:
            anchor = first = previous = row
            sample_count = 1
            segment_gaps = []
            segment_waits = [row["waits"]]
            segment_max_gap = 0.0
            continue
        gap_seconds = row["epoch"]-previous["epoch"]
        invalid_between = [value for value in invalid_epochs
                           if previous["epoch"] <= value <= row["epoch"]]
        if invalid_between:
            finish()
            anchor = first = previous = row
            sample_count = 1
            segment_gaps = []
            segment_waits = [row["waits"]]
            segment_max_gap = 0.0
            continue
        errors_between = [item for item in error_details if item.get("mappedDbEpoch") is not None
                          and previous["epoch"] < item["mappedDbEpoch"] <= row["epoch"]]
        if gap_seconds > wait_gap_limit or errors_between:
            segment_gaps.append({"startUtc": _utc(previous["epoch"]), "endUtc": _utc(row["epoch"]),
                                 "startEpoch": previous["epoch"], "endEpoch": row["epoch"],
                                 "seconds": gap_seconds,
                                 "kind": "sample error between successful observations" if errors_between
                                     else "successful-sample gap exceeds observed-wait limit",
                                 "sampleErrorCount": len(errors_between)})
        segment_max_gap = max(segment_max_gap, gap_seconds)
        previous = row
        sample_count += 1
        segment_waits.append(row["waits"])
    finish()

    validation = {"nativeLogClean": native_log_clean is True,
                  "nativeCountsReconciled": native_counts_reconciled is True}
    threshold = float(minimum_seconds)
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("minimum duration must be nonnegative")
    if best is None:
        best = {"startUtc": None, "endUtc": None, "startEpoch": None, "endEpoch": None,
                "seconds": 0.0, "writerCount": 0, "writers": [], "successfulSamples": 0,
                "timestampMethod": None, "reconstructedStartToleranceSeconds": None,
                "nativeCompletionCountWithin": 0, "allWritersActiveAndWaitingAtSuccessfulSamples": False,
                "observedWaitEventCounts": {}, "sampleErrorsInSpan": [], "unobservedSamplingGaps": [],
                "maximumSuccessfulSampleGapSeconds": 0.0,
                "listedGapPolicy": f"Lists successful-sample intervals exceeding {OBSERVED_WAIT_GAP_SECONDS:g} seconds or containing a mapped sample error; maximumSuccessfulSampleGapSeconds summarizes every adjacent successful observation.",
                "waitStateInterpretation": "No qualifying stable all-writer interval was found."}
    zero_completions = best["nativeCompletionCountWithin"] == 0
    claim = (validation["nativeLogClean"] and validation["nativeCountsReconciled"] and
             best["writerCount"] == 10 and best["seconds"] > threshold and zero_completions)
    return {"claimValid": claim, "thresholdSeconds": threshold,
            "maxStableAllWritersInFlightWithoutCompletionSeconds": best["seconds"],
            "maxStableAllWritersInFlightWithoutCompletion": best,
            "nativeLogValidation": validation,
            "successfulSampleErrorsInWindow": error_details,
            "invalidSuccessfulSamplesInWindow": [
                {key: value for key, value in item.items() if key != "observedEpoch"}
                for item in invalid_successful_samples if item["observedEpoch"] is not None and
                low_epoch <= item["observedEpoch"] <= high_epoch],
            "nativeCompletionsInWindow": sum(low_epoch <= value <= high_epoch for value in completions),
            "strictObservedWaitGapLimitSeconds": wait_gap_limit,
            "claimInterpretation": "A claim requires clean native completion logs with reconciled counts, zero native completions in the candidate interval, and unchanged identities for all ten active/waiting writer statements at every successful sample. Reported sampler gaps are unobserved; their wait-event state is not inferred."}
