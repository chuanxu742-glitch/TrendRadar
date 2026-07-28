import asyncio
import json
import unittest
from unittest import mock
from urllib.error import HTTPError

from mcp_server import server
from mcp_server.services.official_monitor_service import (
    OfficialMonitorAPIError,
    OfficialMonitorService,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return self.payload


class OfficialMonitorServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = OfficialMonitorService(
            base_url="http://monitor.test:8090/", timeout=3
        )

    @mock.patch(
        "mcp_server.services.official_monitor_service.urlopen",
        return_value=FakeResponse({"items": [], "count": 0}),
    )
    def test_sources_query_preserves_role_and_lifecycle_filters(self, mocked_open):
        result = self.service.get_sources(
            states=["active", "quarantined"],
            roles=["current-primary", "candidate"],
            limit=17,
            offset=34,
        )

        request = mocked_open.call_args.args[0]
        self.assertEqual(result["count"], 0)
        self.assertIn("/api/v1/sources?", request.full_url)
        self.assertIn("state=active%2Cquarantined", request.full_url)
        self.assertIn("role=current-primary%2Ccandidate", request.full_url)
        self.assertIn("limit=17", request.full_url)
        self.assertIn("offset=34", request.full_url)
        self.assertEqual(mocked_open.call_args.kwargs["timeout"], 3)

    @mock.patch(
        "mcp_server.services.official_monitor_service.urlopen",
        return_value=FakeResponse({"items": [], "next_cursor": 8}),
    )
    def test_policy_event_view_uses_cursor_without_effective_filter(self, mocked_open):
        self.service.get_policy_changes(view="events", after_cursor=8, limit=20)

        request = mocked_open.call_args.args[0]
        self.assertIn("after=8", request.full_url)
        self.assertIn("limit=20", request.full_url)
        self.assertNotIn("view=effective", request.full_url)

    def test_policy_view_is_validated_locally(self):
        with self.assertRaisesRegex(ValueError, "effective.*events"):
            self.service.get_policy_changes(view="raw")

    @mock.patch(
        "mcp_server.services.official_monitor_service.urlopen",
        return_value=FakeResponse({"counts": {"changes": 1}, "country_groups": []}),
    )
    def test_policy_digest_preserves_date_and_entity_filters(self, mocked_open):
        result = self.service.get_policy_change_digest(
            start_date="2026-04-01",
            end_date="2026-04-30",
            entity_kind="country",
            limit=25,
        )

        request = mocked_open.call_args.args[0]
        self.assertEqual(result["counts"]["changes"], 1)
        self.assertIn("/api/v1/policy-change-digest?", request.full_url)
        self.assertIn("from=2026-04-01", request.full_url)
        self.assertIn("to=2026-04-30", request.full_url)
        self.assertIn("kind=country", request.full_url)
        self.assertIn("limit=25", request.full_url)

    @mock.patch(
        "mcp_server.services.official_monitor_service.urlopen",
        return_value=FakeResponse({"items": [{"active_revision_id": "revision:1"}], "count": 1}),
    )
    def test_current_knowledge_uses_materialized_rules_endpoint(self, mocked_open):
        result = self.service.get_current_knowledge()
        request = mocked_open.call_args.args[0]
        self.assertEqual(result["count"], 1)
        self.assertEqual(request.full_url, "http://monitor.test:8090/api/v1/knowledge-current")

    @mock.patch("mcp_server.services.official_monitor_service.urlopen")
    def test_http_failure_has_actionable_monitor_context(self, mocked_open):
        error = HTTPError(
            "http://monitor.test:8090/api/v1/sources",
            503,
            "Unavailable",
            {},
            None,
        )
        error.read = mock.Mock(return_value=b'{"error":"warming up"}')
        mocked_open.side_effect = error

        with self.assertRaisesRegex(OfficialMonitorAPIError, "HTTP 503.*warming up"):
            self.service.get_sources()


class FakeOfficialMonitor:
    def __init__(self):
        self.calls = []

    def get_sources(self, **kwargs):
        self.calls.append(("sources", kwargs))
        return {"items": [{"source_id": "src:1"}], "count": 1}

    def get_policy_changes(self, **kwargs):
        self.calls.append(("policy", kwargs))
        return {"items": [{"change_id": "change:1"}], "next_cursor": 4}

    def get_policy_change_digest(self, **kwargs):
        self.calls.append(("digest", kwargs))
        return {"counts": {"changes": 1}, "text": "【政策变动汇总】"}

    def get_review_tasks(self, **kwargs):
        self.calls.append(("reviews", kwargs))
        return {"items": [{"task_id": "review:1"}], "count": 1}

    def get_knowledge_updates(self, **kwargs):
        self.calls.append(("knowledge", kwargs))
        return {"items": [{"proposal_id": "proposal:1"}], "count": 1}

    def get_current_knowledge(self, **kwargs):
        self.calls.append(("current_knowledge", kwargs))
        return {"items": [{"active_revision_id": "revision:1"}], "count": 1}


class OfficialMonitorMCPToolTests(unittest.TestCase):
    def setUp(self):
        self.previous = dict(server._tools_instances)
        self.fake = FakeOfficialMonitor()
        server._tools_instances.clear()
        server._tools_instances["official_monitor"] = self.fake

    def tearDown(self):
        server._tools_instances.clear()
        server._tools_instances.update(self.previous)

    def test_all_structured_monitor_interfaces_are_exposed(self):
        sources = asyncio.run(
            server.get_official_sources.fn(
                states=["active"], roles=["current-primary"], limit=9000, offset=10
            )
        )
        changes = asyncio.run(
            server.get_official_policy_changes.fn(
                view="events", after_cursor=4, limit=900
            )
        )
        digest = asyncio.run(
            server.get_official_policy_change_digest.fn(
                start_date="2026-04-01",
                end_date="2026-04-30",
                entity_kind="country",
                limit=20000,
            )
        )
        reviews = asyncio.run(
            server.get_official_review_tasks.fn(statuses=["open"], limit=20, offset=30)
        )
        updates = asyncio.run(
            server.get_official_knowledge_updates.fn(
                statuses=["proposed"], limit=20
            )
        )
        current_knowledge = asyncio.run(server.get_official_current_knowledge.fn())

        self.assertEqual(json.loads(sources)["items"][0]["source_id"], "src:1")
        self.assertEqual(json.loads(changes)["next_cursor"], 4)
        self.assertEqual(json.loads(digest)["counts"]["changes"], 1)
        self.assertEqual(json.loads(reviews)["items"][0]["task_id"], "review:1")
        self.assertEqual(
            json.loads(updates)["items"][0]["proposal_id"], "proposal:1"
        )
        self.assertEqual(
            json.loads(current_knowledge)["items"][0]["active_revision_id"],
            "revision:1",
        )
        self.assertEqual(self.fake.calls[0][1]["limit"], 5000)
        self.assertEqual(self.fake.calls[0][1]["offset"], 10)
        self.assertEqual(self.fake.calls[1][1]["limit"], 500)
        self.assertEqual(self.fake.calls[2][1]["limit"], 10000)
        self.assertEqual(self.fake.calls[2][1]["entity_kind"], "country")
        self.assertEqual(self.fake.calls[3][1]["offset"], 30)


if __name__ == "__main__":
    unittest.main()
