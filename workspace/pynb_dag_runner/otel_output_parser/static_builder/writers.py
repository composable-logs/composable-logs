import json

from typing import Any, List, Optional
from pathlib import Path

#
from otel_output_parser.common_helpers.utils import (
    ensure_dir_exist,
    del_key,
    iso8601_to_epoch_ms,
)
from otel_output_parser.common_helpers.graph import Graph


# --- sinks for processing {pipeline, task, run} summaries ----


class StaticMLFlowDataSink:
    """
    Stateful sink for outputting ML Flow static data
    """

    def __init__(self, output_static_data_json: Optional[Path]):
        self.output_static_data_json: Optional[Path] = output_static_data_json
        self.summaries: List[Any] = []

    def push(self, summary):
        if self.output_static_data_json is None:
            return

        self.summaries.append(
            {
                **summary,
                # overwrite artifacts with list of entries where (the large)
                # "content" data has been deleted to save memory.
                "artifacts": [
                    del_key(entry, "content") for entry in summary["artifacts"]
                ],
            }
        )

    def close(self):
        if self.output_static_data_json is None:
            return

        # Construct graph of parent-child relationships between logged events:
        #
        #     pipeline <= task <= run
        #
        # with <= representing one-to-many relationships (eg. one pipeline may contain
        # many tasks)
        edges = []
        for summary in self.summaries:
            s_parent_id, s_id = summary["parent_id"], summary["id"]

            if s_parent_id is not None:
                assert s_id is not None
                edges.append((s_parent_id, s_id))

        # add `all_children_ids` field to summaries

        g = Graph(set(edges))

        def to_epoch(summary_type, summary_metadata):
            assert ("start_time" in summary_metadata) == (summary_type != "pipeline")
            assert ("end_time" in summary_metadata) == (summary_type != "pipeline")

            if summary_type != "pipeline":
                return {
                    **summary_metadata,
                    "start_time": iso8601_to_epoch_ms(summary_metadata["start_time"]),
                    "end_time": iso8601_to_epoch_ms(summary_metadata["end_time"]),
                }
            else:
                # TODO: see below
                return summary_metadata

        aug_summaries = {
            summary["id"]: {
                **del_key(summary, "id"),
                "metadata": to_epoch(summary["type"], summary["metadata"]),
                # `all_children_ids` key and will be removed below before
                # writing output to disk.
                "all_children_ids": list(g.all_children_of(summary["id"])),
            }
            for summary in self.summaries
        }

        # Manually fill in pipeline run times from min-max of children summaries
        # // TODO: this would not be necessary after pipeline level-tasks have
        # otel spans, see above.
        for _, summary in aug_summaries.items():
            if summary["type"] == "pipeline":
                all_children = [aug_summaries[k] for k in summary["all_children_ids"]]
                assert (
                    len(all_children) > 0
                ), "pipeline tasks should have subtasks -- delete these if needed?"

                summary["metadata"]["start_time"] = min(
                    s["metadata"]["start_time"] for s in all_children
                )
                summary["metadata"]["end_time"] = max(
                    s["metadata"]["end_time"] for s in all_children
                )
        # -----

        # remove all `all_children_ids`-keys since no longer needed
        for _, v in aug_summaries.items():
            del v["all_children_ids"]

        # -----

        ensure_dir_exist(self.output_static_data_json).write_text(
            json.dumps(aug_summaries, indent=2)
        )


def write_attachment_sink(output_dir: Optional[Path], summary):
    """
    Stateless sink: write attachments in a {pipeline, task, run}-summary to
    output directory.

    After a summary object has been processed, all attachments can be released
    from memory.
    """
    if output_dir is None:
        return

    for artifact in summary["artifacts"]:

        output_path = output_dir / summary["artifacts_location"] / artifact["name"]
        content = artifact["content"]

        if isinstance(content, bytes):
            ensure_dir_exist(output_path).write_bytes(content)
        elif isinstance(content, str):
            ensure_dir_exist(output_path).write_text(content)
        else:
            raise Exception(f"Unknown type {type(content)}")
