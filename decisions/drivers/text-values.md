# Parameters and results as text

A driver sends every parameter as text without a declared type and returns every value as text; the core encodes its parameters as text, each statement casts them from text, as `$n::text::int`, and the core reads the values of its rows itself.
The SQL alone decides the types, so the core behaves the same on postgres.js and node-postgres, and on clients that an application configured with its own parsers or serializers.

Letting the client serialize parameters and parse results needs no decoding in the core, and gives dates and numbers directly.
But the clients differ: postgres.js serializes a parameter by the type that PostgreSQL describes, so the string `"true"` of a `boolean` parameter becomes false and a JSON string of a `jsonb` parameter is encoded again, while node-postgres sends strings as they are; a client of the application can also parse `bigint` into numbers that lose precision.

Reading job rows needs timestamps: the statements select them as seconds since the epoch, since the text of a `timestamptz` follows `DateStyle` and `TimeZone` of the connection.
