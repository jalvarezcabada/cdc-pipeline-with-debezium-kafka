from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("table.exec.source.idle-timeout", "5000")
t_env.get_config().set("execution.checkpointing.interval", "30s")
t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
t_env.get_config().set("state.checkpoints.dir", "file:///opt/flink/checkpoints")

t_env.execute_sql("""
    CREATE TABLE orders_cdc (
        payload ROW<
            before ROW<id INT, customer_name STRING, product STRING, quantity INT, total STRING, status STRING>,
            after  ROW<id INT, customer_name STRING, product STRING, quantity INT, total STRING, status STRING>,
            op     STRING,
            ts_ms  BIGINT
        >,
        event_time AS TO_TIMESTAMP_LTZ(payload.ts_ms, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'debezium.public.orders',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'pyflink-cdc-group',
        'properties.enable.auto.commit' = 'true',
        'scan.startup.mode' = 'group-offsets',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
""")

t_env.execute_sql("""
    CREATE TABLE orders_window_result (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_orders BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

t_env.execute_sql("""
    INSERT INTO orders_window_result
    SELECT
        window_start,
        window_end,
        COUNT(*) AS total_orders
    FROM TABLE(
        TUMBLE(
            TABLE orders_cdc,
            DESCRIPTOR(event_time),
            INTERVAL '30' SECOND
        )
    )
    GROUP BY window_start, window_end
""").wait()
