TRUNCATE TABLE benchmark.events_route_first;
TRUNCATE TABLE benchmark.events_time_first;


/*
Generate one million deterministic synthetic events.

The timestamps cover approximately 93 days.
All generated values are reproducible.
*/
INSERT INTO benchmark.events_route_first
SELECT
    toDateTime64('2026-01-01 00:00:00', 3, 'UTC')
        + toIntervalSecond(toInt64(number) * 8) AS event_time,

    reinterpretAsUUID(
        MD5(concat('event-', toString(number)))
    ) AS event_id,

    toUInt64((number % 100000) + 1) AS user_id,

    reinterpretAsUUID(
        MD5(concat('session-', toString(intDiv(number, 5))))
    ) AS session_id,

    arrayElement(
        ['THR', 'THR', 'THR', 'THR', 'IKA', 'IKA',
         'IKA', 'IKA', 'MHD', 'MHD', 'SYZ', 'TBZ'],
        toUInt8((number % 12) + 1)
    ) AS origin,

    arrayElement(
        ['IST', 'DXB', 'DOH', 'FRA', 'LHR', 'CDG',
         'IST', 'DXB', 'IST', 'DXB', 'DOH', 'IST'],
        toUInt8((number % 12) + 1)
    ) AS destination,

    if(number % 10 = 0, 'business', 'economy') AS cabin_class,

    toUInt8((number % 4) + 1) AS adults,

    toUInt8(if(number % 5 = 0, 1, 0)) AS children,

    toUInt16((number % 77) + 4) AS result_count,

    round(
        toFloat64(120 + (number % 600))
        * if(number % 10 = 0, 2.5, 1.0),
        2
    ) AS min_price,

    toUInt32(100 + (number % 900)) AS response_time_ms,

    arrayElement(
        ['mobile', 'desktop', 'tablet'],
        toUInt8((number % 3) + 1)
    ) AS device,

    arrayElement(
        ['organic', 'direct', 'paid', 'affiliate'],
        toUInt8((number % 4) + 1)
    ) AS source

FROM numbers(1000000);


/*
Copy the exact same rows into the time-first table.
ClickHouse will physically arrange them according to the
different ORDER BY expression of the destination table.
*/
INSERT INTO benchmark.events_time_first
SELECT *
FROM benchmark.events_route_first;