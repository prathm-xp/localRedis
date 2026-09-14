"""End-to-end tests for publish/subscribe."""

from __future__ import annotations

import time
from typing import Any

import pytest

from tests.integration.client import RespClient, Status

pytestmark = pytest.mark.integration


def subscribe(connection: RespClient, *channels: str) -> list[Any]:
    """Subscribe and consume the one confirmation per channel."""
    connection.send_command("SUBSCRIBE", *channels)
    return [connection.read_reply() for _ in channels]


class TestSubscribe:
    def test_confirmation_per_channel(self, second_client):
        replies = subscribe(second_client, "news", "sport")
        assert replies == [
            [b"subscribe", b"news", 1],
            [b"subscribe", b"sport", 2],
        ]

    def test_unsubscribe(self, second_client):
        subscribe(second_client, "news")
        assert second_client.command("UNSUBSCRIBE", "news") == [b"unsubscribe", b"news", 0]

    def test_unsubscribe_from_everything(self, second_client):
        subscribe(second_client, "a", "b")
        second_client.send_command("UNSUBSCRIBE")
        replies = [second_client.read_reply() for _ in range(2)]
        assert {reply[1] for reply in replies} == {b"a", b"b"}
        assert replies[-1][2] == 0

    def test_unsubscribe_with_no_subscriptions(self, second_client):
        assert second_client.command("UNSUBSCRIBE") == [b"unsubscribe", None, 0]

    def test_psubscribe(self, second_client):
        second_client.send_command("PSUBSCRIBE", "news.*")
        assert second_client.read_reply() == [b"psubscribe", b"news.*", 1]

    def test_punsubscribe(self, second_client):
        second_client.send_command("PSUBSCRIBE", "news.*")
        second_client.read_reply()
        assert second_client.command("PUNSUBSCRIBE", "news.*") == [b"punsubscribe", b"news.*", 0]


class TestPublish:
    def test_publish_with_no_subscribers(self, client):
        assert client.command("PUBLISH", "empty", "message") == 0

    def test_message_is_delivered(self, client, second_client):
        subscribe(second_client, "news")
        assert client.command("PUBLISH", "news", "hello") == 1
        assert second_client.read_reply() == [b"message", b"news", b"hello"]

    def test_binary_message(self, client, second_client):
        subscribe(second_client, "news")
        payload = bytes(range(256))
        client.command("PUBLISH", "news", payload)
        assert second_client.read_reply() == [b"message", b"news", payload]

    def test_delivered_to_every_subscriber(self, shared_server, client):
        subscribers = [shared_server.client() for _ in range(3)]
        try:
            for connection in subscribers:
                subscribe(connection, "news")
            assert client.command("PUBLISH", "news", "hello") == 3
            for connection in subscribers:
                assert connection.read_reply() == [b"message", b"news", b"hello"]
        finally:
            for connection in subscribers:
                connection.close()

    def test_pattern_subscriber_receives_pmessage(self, client, second_client):
        second_client.send_command("PSUBSCRIBE", "news.*")
        second_client.read_reply()
        assert client.command("PUBLISH", "news.tech", "hello") == 1
        assert second_client.read_reply() == [b"pmessage", b"news.*", b"news.tech", b"hello"]

    def test_pattern_that_does_not_match_receives_nothing(self, client, second_client):
        second_client.send_command("PSUBSCRIBE", "news.*")
        second_client.read_reply()
        assert client.command("PUBLISH", "sport.football", "hello") == 0

    def test_channel_and_pattern_both_deliver(self, client, second_client):
        subscribe(second_client, "news.tech")
        second_client.send_command("PSUBSCRIBE", "news.*")
        second_client.read_reply()
        # A client subscribed both ways is counted, and delivered to, twice.
        assert client.command("PUBLISH", "news.tech", "hello") == 2
        kinds = {second_client.read_reply()[0] for _ in range(2)}
        assert kinds == {b"message", b"pmessage"}

    def test_unsubscribed_client_stops_receiving(self, client, second_client):
        subscribe(second_client, "news")
        second_client.command("UNSUBSCRIBE", "news")
        assert client.command("PUBLISH", "news", "hello") == 0

    def test_disconnect_removes_the_subscription(self, shared_server, client):
        subscriber = shared_server.client()
        subscribe(subscriber, "news")
        assert client.command("PUBLISH", "news", "x") == 1
        subscriber.close()
        time.sleep(0.2)
        assert client.command("PUBLISH", "news", "x") == 0


class TestSubscribeMode:
    def test_other_commands_are_refused(self, second_client):
        subscribe(second_client, "news")
        reply = second_client.command("GET", "key")
        assert reply.startswith("ERR Can't execute 'get'")

    def test_ping_is_allowed_and_replies_as_an_array(self, second_client):
        subscribe(second_client, "news")
        assert second_client.command("PING") == [b"pong", b""]

    def test_ping_with_a_message_in_subscribe_mode(self, second_client):
        subscribe(second_client, "news")
        assert second_client.command("PING", "hi") == [b"pong", b"hi"]

    def test_subscribe_is_allowed(self, second_client):
        subscribe(second_client, "news")
        assert subscribe(second_client, "sport") == [[b"subscribe", b"sport", 2]]

    def test_reset_leaves_subscribe_mode(self, second_client):
        subscribe(second_client, "news")
        assert second_client.command("RESET") == Status("RESET")
        assert second_client.command("PING") == Status("PONG")


class TestPubsubIntrospection:
    def test_channels(self, client, second_client):
        subscribe(second_client, "news", "sport")
        assert sorted(client.command("PUBSUB", "CHANNELS")) == [b"news", b"sport"]

    def test_channels_with_a_pattern(self, client, second_client):
        subscribe(second_client, "news.a", "sport.b")
        assert client.command("PUBSUB", "CHANNELS", "news.*") == [b"news.a"]

    def test_numsub(self, client, second_client):
        subscribe(second_client, "news")
        assert client.command("PUBSUB", "NUMSUB", "news", "empty") == [b"news", 1, b"empty", 0]

    def test_numpat(self, client, second_client):
        second_client.send_command("PSUBSCRIBE", "a.*", "b.*")
        second_client.read_reply()
        second_client.read_reply()
        assert client.command("PUBSUB", "NUMPAT") == 2

    def test_unknown_subcommand(self, client):
        assert client.command("PUBSUB", "BOGUS").startswith("ERR Unknown PUBSUB subcommand")
