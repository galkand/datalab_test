from __future__ import annotations

import math
import urllib.parse
import urllib.request
import urllib.error
from typing import Iterable, Iterator, Optional, Callable, Sequence

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from .common import Env, log_kv


def _ch_url(env: Env) -> str:
    return (
        f"http://{env.ch_host}:{env.ch_port}/?"
        + urllib.parse.urlencode({"database": env.ch_db, "user": env.ch_user, "password": env.ch_password})
    )


def ch_exec(sql: str, env: Optional[Env] = None, timeout_s: int = 60) -> str:
    env = env or Env()
    url = _ch_url(env)
    data = sql.encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8", errors="replace")


def publish_df_json_each_row_partitioned(
    df: DataFrame,
    *,
    table: str,
    partition_col: str,
    truncate: bool = True,
    batch_rows: int = 10_000,
    env: Optional[Env] = None,
) -> None:
    env = env or Env()

    if truncate:
        log_kv(ch="truncate", table=table)
        ch_exec(f"TRUNCATE TABLE {table}", env=env)

    url = _ch_url(env)

    def post_payload(payload: str) -> None:
        data = payload.encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                _ = resp.read()
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            raise RuntimeError(f"ClickHouse HTTP {e.code} {e.reason}; body={body[:4000]}") from e

    def chunks(it: Iterator[str], n: int) -> Iterator[list[str]]:
        buf: list[str] = []
        for x in it:
            buf.append(x)
            if len(buf) >= n:
                yield buf
                buf = []
        if buf:
            yield buf

    def publish_partition(rows_iter: Iterator[tuple[str, str]]) -> None:
        current_key: Optional[str] = None
        buf: list[str] = []

        def flush() -> None:
            nonlocal buf, current_key
            if not buf or current_key is None:
                return
            payload = f"INSERT INTO {table} SETTINGS input_format_skip_unknown_fields=1 FORMAT JSONEachRow\n" + "\n".join(buf) + "\n"
            post_payload(payload)
            buf = []

        for key, json_row in rows_iter:
            if current_key is None:
                current_key = key

            if key != current_key:
                flush()
                current_key = key

            buf.append(json_row)
            if len(buf) >= batch_rows:
                flush()

        flush()

    log_kv(ch="insert_partitioned", table=table, partition_col=partition_col, batch_rows=batch_rows)

    keyed = (
        df
        .repartition(F.col(partition_col))
        .sortWithinPartitions(partition_col)
        .select(F.col(partition_col).cast("string").alias("__p"), F.to_json(F.struct(*df.columns)).alias("__j"))
    )

    keyed.rdd.map(lambda r: (r["__p"], r["__j"])).foreachPartition(publish_partition)

    try:
        out = ch_exec(f"SELECT count() FROM {table} FORMAT TabSeparated", env=env).strip()
        log_kv(ch="count", table=table, rows=out)
    except Exception as e:
        log_kv(ch="count_failed", table=table, err=str(e))


def publish_df_json_each_row(
    df: DataFrame,
    *,
    table: str,
    truncate: bool = True,
    batch_rows: int = 10_000,
    env: Optional[Env] = None,
    require_table_exists: bool = True,
) -> None:
    env = env or Env()

    if truncate:
        log_kv(ch="truncate", table=table)
        ch_exec(f"TRUNCATE TABLE {table}", env=env)

    url = _ch_url(env)

    def post_payload(payload: str) -> None:
        data = payload.encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=120) as resp:
            _ = resp.read()

    def chunks(it: Iterator[str], n: int) -> Iterator[list[str]]:
        buf: list[str] = []
        for x in it:
            buf.append(x)
            if len(buf) >= n:
                yield buf
                buf = []
        if buf:
            yield buf

    def publish_partition(rows: Iterable[str]) -> None:
        it = iter(rows)
        for batch in chunks(it, batch_rows):
            payload = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(batch) + "\n"
            post_payload(payload)

    log_kv(ch="insert", table=table, mode="foreachPartition", batch_rows=batch_rows)
    df.toJSON().foreachPartition(publish_partition)

    try:
        out = ch_exec(f"SELECT count() FROM {table} FORMAT TabSeparated", env=env).strip()
        log_kv(ch="count", table=table, rows=out)
    except Exception as e:
        log_kv(ch="count_failed", table=table, err=str(e))

