"""End-to-end tests for connection handling and the string/generic commands."""

from __future__ import annotations

import pytest

from tests.integration.client import Error, RespClient, Status

pytestmark = pytest.mark.integration


class TestConnection:
    def test_ping(self, client):
        assert client.command("PING") == Status("PONG")

    def test_ping_with_message(self, client):
        assert client.command("PING", "hello") == b"hello"

    def test_echo(self, client):
        assert client.command("ECHO", "hey") == b"hey"

    def test_many_commands_on_one_connection(self, client):
        for index in range(50):
            assert client.command("SET", f"k{index}", index) == Status("OK")
        for index in range(50):
            assert client.command("GET", f"k{index}") == str(index).encode()

    def test_pipelined_commands(self, client):
        client.send_command("SET", "a", "1")
        client.send_command("SET", "b", "2")
        client.send_command("GET", "a")
        client.send_command("GET", "b")
        assert client.read_reply() == Status("OK")
        assert client.read_reply() == Status("OK")
        assert client.read_reply() == b"1"
        assert client.read_reply() == b"2"

    def test_inline_command(self, client):
        client.send_raw(b"PING\r\n")
        assert client.read_reply() == Status("PONG")

    def test_inline_command_with_quotes(self, client):
        client.send_raw(b'SET greeting "hello world"\r\n')
        assert client.read_reply() == Status("OK")
        assert client.command("GET", "greeting") == b"hello world"

    def test_select_database_zero(self, client):
        assert client.command("SELECT", 0) == Status("OK")

    def test_select_other_database_is_refused(self, client):
        assert client.command("SELECT", 1) == Error("ERR DB index is out of range")

    def test_client_id_is_unique(self, client, second_client):
        assert client.command("CLIENT", "ID") != second_client.command("CLIENT", "ID")

    def test_client_setname_and_getname(self, client):
        assert client.command("CLIENT", "GETNAME") is None
        assert client.command("CLIENT", "SETNAME", "worker") == Status("OK")
        assert client.command("CLIENT", "GETNAME") == b"worker"

    def test_hello_2_is_accepted(self, client):
        reply = client.command("HELLO", 2)
        fields = dict(zip(reply[::2], reply[1::2], strict=True))
        assert fields[b"proto"] == 2
        assert fields[b"server"] == b"pyredis"

    def test_hello_3_is_refused(self, client):
        assert client.command("HELLO", 3).startswith("NOPROTO")

    def test_quit_closes_the_connection(self, shared_server):
        connection = shared_server.client()
        assert connection.command("QUIT") == Status("OK")
        assert connection.read_until_eof() == b""
        connection.close()


class TestMultipleClients:
    def test_clients_share_the_keyspace(self, client, second_client):
        client.command("SET", "shared", "value")
        assert second_client.command("GET", "shared") == b"value"

    def test_many_concurrent_connections(self, shared_server):
        connections = [shared_server.client() for _ in range(25)]
        try:
            for index, connection in enumerate(connections):
                connection.command("SET", f"conn:{index}", index)
            for index, connection in enumerate(connections):
                assert connection.command("GET", f"conn:{index}") == str(index).encode()
        finally:
            for connection in connections:
                connection.close()

    def test_disconnect_does_not_disturb_other_clients(self, client, shared_server):
        transient = shared_server.client()
        transient.command("SET", "left-behind", "1")
        transient.close()
        assert client.command("GET", "left-behind") == b"1"


class TestStrings:
    def test_get_missing_key(self, client):
        assert client.command("GET", "absent") is None

    def test_set_and_get(self, client):
        assert client.command("SET", "name", "aryan") == Status("OK")
        assert client.command("GET", "name") == b"aryan"

    def test_overwrite(self, client):
        client.command("SET", "k", "first")
        client.command("SET", "k", "second")
        assert client.command("GET", "k") == b"second"

    def test_empty_value(self, client):
        client.command("SET", "k", "")
        assert client.command("GET", "k") == b""

    def test_binary_safe_values(self, client):
        payload = bytes(range(256))
        client.command("SET", b"binary", payload)
        assert client.command("GET", b"binary") == payload

    def test_binary_safe_keys(self, client):
        client.command("SET", b"\x00\xff", b"value")
        assert client.command("GET", b"\x00\xff") == b"value"

    def test_large_value(self, client):
        payload = b"x" * (1024 * 1024)
        client.command("SET", "big", payload)
        assert client.command("GET", "big") == payload

    def test_set_nx(self, client):
        assert client.command("SET", "k", "first", "NX") == Status("OK")
        assert client.command("SET", "k", "second", "NX") is None
        assert client.command("GET", "k") == b"first"

    def test_set_xx(self, client):
        assert client.command("SET", "k", "v", "XX") is None
        client.command("SET", "k", "first")
        assert client.command("SET", "k", "second", "XX") == Status("OK")

    def test_set_get_option(self, client):
        assert client.command("SET", "k", "first", "GET") is None
        assert client.command("SET", "k", "second", "GET") == b"first"

    def test_setnx(self, client):
        assert client.command("SETNX", "k", "v") == 1
        assert client.command("SETNX", "k", "other") == 0

    def test_getdel(self, client):
        client.command("SET", "k", "v")
        assert client.command("GETDEL", "k") == b"v"
        assert client.command("EXISTS", "k") == 0

    def test_mset_and_mget(self, client):
        assert client.command("MSET", "a", "1", "b", "2") == Status("OK")
        assert client.command("MGET", "a", "b", "missing") == [b"1", b"2", None]

    def test_append(self, client):
        assert client.command("APPEND", "k", "foo") == 3
        assert client.command("APPEND", "k", "bar") == 6
        assert client.command("GET", "k") == b"foobar"

    def test_strlen(self, client):
        client.command("SET", "k", "hello")
        assert client.command("STRLEN", "k") == 5
        assert client.command("STRLEN", "missing") == 0

    def test_getrange(self, client):
        client.command("SET", "k", "Hello World")
        assert client.command("GETRANGE", "k", 0, 4) == b"Hello"
        assert client.command("GETRANGE", "k", -5, -1) == b"World"
        assert client.command("GETRANGE", "k", 0, -1) == b"Hello World"


class TestCounters:
    def test_incr_creates_at_one(self, client):
        assert client.command("INCR", "counter") == 1
        assert client.command("INCR", "counter") == 2
        assert client.command("GET", "counter") == b"2"

    def test_decr(self, client):
        client.command("SET", "counter", 10)
        assert client.command("DECR", "counter") == 9

    def test_incrby_and_decrby(self, client):
        assert client.command("INCRBY", "counter", 5) == 5
        assert client.command("DECRBY", "counter", 3) == 2

    def test_incr_on_non_numeric_value(self, client):
        client.command("SET", "k", "abc")
        assert client.command("INCR", "k") == Error("ERR value is not an integer or out of range")

    def test_incr_on_padded_value_is_an_error(self, client):
        # Redis parses with strtoll, which rejects the leading space.
        client.command("SET", "k", " 1")
        assert client.command("INCR", "k") == Error("ERR value is not an integer or out of range")

    def test_incr_overflow(self, client):
        client.command("SET", "k", 2**63 - 1)
        assert client.command("INCR", "k") == Error("ERR increment or decrement would overflow")

    def test_incr_preserves_ttl(self, client):
        client.command("SET", "k", 1, "EX", 100)
        client.command("INCR", "k")
        assert 0 < client.command("TTL", "k") <= 100


class TestGenericKeyCommands:
    def test_exists(self, client):
        client.command("SET", "a", "1")
        assert client.command("EXISTS", "a") == 1
        assert client.command("EXISTS", "missing") == 0
        assert client.command("EXISTS", "a", "a", "missing") == 2

    def test_del(self, client):
        client.command("MSET", "a", "1", "b", "2")
        assert client.command("DEL", "a", "b", "missing") == 2
        assert client.command("EXISTS", "a") == 0

    def test_unlink_behaves_like_del(self, client):
        client.command("SET", "a", "1")
        assert client.command("UNLINK", "a") == 1

    def test_type(self, client):
        client.command("SET", "s", "v")
        client.command("RPUSH", "l", "v")
        client.command("XADD", "st", "1-1", "f", "v")
        client.command("ZADD", "z", 1, "m")
        assert client.command("TYPE", "s") == Status("string")
        assert client.command("TYPE", "l") == Status("list")
        assert client.command("TYPE", "st") == Status("stream")
        assert client.command("TYPE", "z") == Status("zset")
        assert client.command("TYPE", "missing") == Status("none")

    def test_keys_pattern(self, client):
        client.command("MSET", "user:1", "a", "user:2", "b", "post:1", "c")
        assert sorted(client.command("KEYS", "user:*")) == [b"user:1", b"user:2"]
        assert sorted(client.command("KEYS", "*")) == [b"post:1", b"user:1", b"user:2"]
        assert client.command("KEYS", "nothing*") == []

    def test_dbsize(self, client):
        assert client.command("DBSIZE") == 0
        client.command("MSET", "a", "1", "b", "2")
        assert client.command("DBSIZE") == 2

    def test_rename(self, client):
        client.command("SET", "old", "v")
        assert client.command("RENAME", "old", "new") == Status("OK")
        assert client.command("GET", "new") == b"v"
        assert client.command("EXISTS", "old") == 0

    def test_rename_missing_key(self, client):
        assert client.command("RENAME", "missing", "new") == Error("ERR no such key")

    def test_randomkey_on_empty_keyspace(self, client):
        assert client.command("RANDOMKEY") is None

    def test_flushall(self, client):
        client.command("MSET", "a", "1", "b", "2")
        assert client.command("FLUSHALL") == Status("OK")
        assert client.command("DBSIZE") == 0


class TestWrongType:
    def test_get_on_a_list(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("GET", "l").startswith("WRONGTYPE")

    def test_rpush_on_a_string(self, client):
        client.command("SET", "s", "v")
        assert client.command("RPUSH", "s", "a").startswith("WRONGTYPE")

    def test_xadd_on_a_string(self, client):
        client.command("SET", "s", "v")
        assert client.command("XADD", "s", "*", "f", "v").startswith("WRONGTYPE")

    def test_zadd_on_a_string(self, client):
        client.command("SET", "s", "v")
        assert client.command("ZADD", "s", 1, "m").startswith("WRONGTYPE")

    def test_mget_reports_nil_rather_than_erroring(self, client):
        client.command("SET", "s", "v")
        client.command("RPUSH", "l", "a")
        assert client.command("MGET", "s", "l") == [b"v", None]


class TestServerIntrospection:
    def test_info_has_the_expected_sections(self, client):
        info = client.command("INFO").decode()
        for section in ("# Server", "# Clients", "# Stats", "# Replication"):
            assert section in info

    def test_info_single_section(self, client):
        info = client.command("INFO", "replication").decode()
        assert "# Replication" in info
        assert "# Server" not in info

    def test_info_reports_master_role_by_default(self, client):
        assert "role:master" in client.command("INFO", "replication").decode()

    def test_config_get_dir_and_dbfilename(self, client, shared_server):
        reply = client.command("CONFIG", "GET", "dir")
        assert reply[0] == b"dir"
        assert reply[1].decode() == str(shared_server.directory)
        assert client.command("CONFIG", "GET", "dbfilename")[1] == b"dump.rdb"

    def test_config_get_glob(self, client):
        reply = client.command("CONFIG", "GET", "d*")
        names = set(reply[::2])
        assert names == {b"dir", b"dbfilename"}

    def test_config_set_is_reported_as_unsupported(self, client):
        assert client.command("CONFIG", "SET", "dir", "/tmp").startswith("ERR Unknown CONFIG")

    def test_command_count(self, client):
        assert client.command("COMMAND", "COUNT") > 50

    def test_command_docs_is_empty(self, client):
        assert client.command("COMMAND", "DOCS") == []

    def test_time(self, client):
        seconds, microseconds = client.command("TIME")
        assert int(seconds) > 1_600_000_000
        assert 0 <= int(microseconds) < 1_000_000


class TestErrorHandling:
    def test_unknown_command(self, client):
        reply = client.command("NOSUCHCOMMAND", "a", "b")
        assert reply == Error(
            "ERR unknown command 'NOSUCHCOMMAND', with args beginning with: 'a', 'b', "
        )

    def test_wrong_arity_too_few(self, client):
        assert client.command("GET") == Error("ERR wrong number of arguments for 'get' command")

    def test_wrong_arity_too_many(self, client):
        assert client.command("GET", "a", "b") == Error(
            "ERR wrong number of arguments for 'get' command"
        )

    def test_set_with_unknown_option(self, client):
        assert client.command("SET", "k", "v", "BOGUS") == Error("ERR syntax error")

    def test_expire_with_non_integer(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIRE", "k", "abc") == Error(
            "ERR value is not an integer or out of range"
        )

    def test_connection_survives_an_error(self, client):
        client.command("GET")
        assert client.command("PING") == Status("PONG")

    def test_command_names_are_case_insensitive(self, client):
        assert client.command("sEt", "k", "v") == Status("OK")
        assert client.command("get", "k") == b"v"


class TestMaxClients:
    def test_connection_beyond_the_limit_is_refused(self, server_factory):
        server = server_factory("--maxclients", "2")
        accepted = [server.client(), server.client()]
        try:
            for connection in accepted:
                assert connection.command("PING") == Status("PONG")

            extra = RespClient("127.0.0.1", server.port)
            try:
                assert extra.read_reply() == Error("ERR max number of clients reached")
            finally:
                extra.close()
        finally:
            for connection in accepted:
                connection.close()
