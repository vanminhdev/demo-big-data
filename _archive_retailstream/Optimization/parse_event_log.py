"""
Buoi 15 - Optimization: doc lai Spark event log (JSON Lines, ghi boi
spark.eventLog.enabled=true khi chay 3 job demo trong Spark/jobs/) de lay so lieu
task/stage THAT SU sau khi driver da dung han - tranh race-condition khi scrape
REST API :4040 dang song (khong co Spark History Server duoc cau hinh trong du an nay).

Cach dung:
    python parse_event_log.py <duong_dan_event_log_file> [--stage-name-contains TEXT]

In ra, theo tung stage (sap xep theo stageId):
  - stage id, ten (rut gon), so task, tong input bytes doc, tong shuffle read/write bytes
  - min/median/max task duration (ms) va min/median/max task input+shuffle-read bytes
  -> dung de phat hien data skew (task lech han cac task khac) va do shuffle read/write.

Khong dung thu vien ngoai (chi json/statistics thu vien chuan), chay truc tiep tren host
bang python co san (khong can PySpark o host).
"""
import json
import statistics
import sys


def load_events(path):
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_event_log.py <event_log_path> [--stage-name-contains TEXT]")
        sys.exit(1)
    path = sys.argv[1]
    name_filter = None
    if "--stage-name-contains" in sys.argv:
        idx = sys.argv.index("--stage-name-contains")
        name_filter = sys.argv[idx + 1]

    events = load_events(path)

    stage_names = {}
    stage_submit_time = {}
    stage_complete_time = {}
    for e in events:
        et = e.get("Event")
        if et == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            stage_names[si["Stage ID"]] = si["Stage Name"]
            stage_submit_time[si["Stage ID"]] = si.get("Submission Time")
        elif et == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            stage_names[si["Stage ID"]] = si["Stage Name"]
            stage_complete_time[si["Stage ID"]] = si.get("Completion Time")

    # collect per-task metrics grouped by stage id
    tasks_by_stage = {}
    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd":
            continue
        sid = e["Stage ID"]
        ti = e["Task Info"]
        tm = e.get("Task Metrics", {})
        duration_ms = ti["Finish Time"] - ti["Launch Time"]
        input_bytes = tm.get("Input Metrics", {}).get("Bytes Read", 0)
        shuffle_read = tm.get("Shuffle Read Metrics", {})
        shuffle_read_bytes = shuffle_read.get("Remote Bytes Read", 0) + shuffle_read.get(
            "Local Bytes Read", 0
        )
        shuffle_write_bytes = tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
        output_bytes = tm.get("Output Metrics", {}).get("Bytes Written", 0)
        tasks_by_stage.setdefault(sid, []).append(
            {
                "taskId": ti["Task ID"],
                "index": ti["Index"],
                "executorId": ti["Executor ID"],
                "duration_ms": duration_ms,
                "input_bytes": input_bytes,
                "shuffle_read_bytes": shuffle_read_bytes,
                "shuffle_write_bytes": shuffle_write_bytes,
                "output_bytes": output_bytes,
            }
        )

    print(f"# Event log: {path}")
    print(f"# So stage co task end event: {len(tasks_by_stage)}")
    print()

    for sid in sorted(tasks_by_stage.keys()):
        name = stage_names.get(sid, "?")
        if name_filter and name_filter not in name:
            continue
        tasks = tasks_by_stage[sid]
        durations = [t["duration_ms"] for t in tasks]
        in_bytes = [t["input_bytes"] for t in tasks]
        shr_bytes = [t["shuffle_read_bytes"] for t in tasks]
        shw_bytes = [t["shuffle_write_bytes"] for t in tasks]
        total_shuffle_read = sum(shr_bytes)
        total_shuffle_write = sum(shw_bytes)
        total_input = sum(in_bytes)

        print(f"## Stage {sid}: {name}")
        print(f"   numTasks={len(tasks)}")
        print(
            f"   duration_ms  min={min(durations)} median={statistics.median(durations):.0f} "
            f"max={max(durations)}"
        )
        if any(in_bytes):
            print(
                f"   input_bytes  min={min(in_bytes)} median={statistics.median(in_bytes):.0f} "
                f"max={max(in_bytes)} total={total_input}"
            )
        if total_shuffle_read:
            print(
                f"   shuffle_read_bytes  min={min(shr_bytes)} "
                f"median={statistics.median(shr_bytes):.0f} max={max(shr_bytes)} "
                f"total={total_shuffle_read}"
            )
        if total_shuffle_write:
            print(
                f"   shuffle_write_bytes  min={min(shw_bytes)} "
                f"median={statistics.median(shw_bytes):.0f} max={max(shw_bytes)} "
                f"total={total_shuffle_write}"
            )
        # top 3 task lech nhat theo duration, de lam bang chung data-skew truc quan
        top = sorted(tasks, key=lambda t: -t["duration_ms"])[:3]
        print("   top3 task theo duration (taskId, duration_ms, input_bytes, shuffle_read_bytes):")
        for t in top:
            print(
                f"     task={t['taskId']} dur={t['duration_ms']} "
                f"input={t['input_bytes']} shuffle_read={t['shuffle_read_bytes']}"
            )
        print()


if __name__ == "__main__":
    main()
