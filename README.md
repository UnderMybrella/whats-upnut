# What's UpNut?

A wrapper project around [eventually](https://allie-signet.stoplight.io/docs/eventually/reference/eventually-api.v1.yaml) that adds in the up-to-date* nut (and soon - scale) counts, and a few associated utilities.

Endpoints

### GET /library
Returns all library books and chapters that we're aware of.

### GET /events
Queries [eventually](https://allie-signet.stoplight.io/docs/eventually/reference/eventually-api.v1.yaml/paths/~1events/get) with the provided query parameters, but updates the nuts.

An additional parameter `time` can be provided to limit nut counts to before that time (Supports either unix long or ISO time)

### GET /nuts
Similar to /events, but returns nuts as a list of instances rather than a static coutn

### GET /feed/{hot|top}/{global|team|game|player}
Get hot or top events from the following categories; supports the following query parameters:

- `time` - Time to filter at (long or ISO time)
- `one_of_providers` - Filter by nut provider(s) (Official from the site is `"7fcb63bc-11f2-40b9-b465-f1d458692a63"`)
- `none_of_providers` - Filter by not nut provider(s) (Official from the site is `"7fcb63bc-11f2-40b9-b465-f1d458692a63"`)
- `one_of_sources` - Filter by user(s) that gave a nut to this event
- `none_of_sources` - Filter by not user(s) that gave a nut to this event

- Metadata filters:
    - `season`
    - `tournament`
    - `type`
    - `day`
    - `phase`
    - `category`
    
- `limit` - Limit on number of results to return

- `provider` and `source` allow you to include the `upnut` property in metadata if this user gave a nut or scale to this event.

## GET /feed/{global|team|player|game}

Get events from either ourselves or eventually, filtering and sorting similar to how the official endpoint would.

## Authorization Endpoints
These endpoints require valid authorization tokens, which are [JWT](https://jwt.io/) tokens with the following requirements:

- `iss` must be set to your provider ID.
- The key must be signed with your provider's RSA Private Key.

### PUT /{feed_id}/{provider}
Upnut `feed_id` with 1 nut from the provider.

Accepts a query parameter for source and/or time. 

Returns a 201 on success, 400 when already upshelled, 401 when unauthorised.

### DELETE /{feed_id}/{provider}
Deletes `feed_id` upnut from the provider.

Accepts a query parameter for source. Note that if no source is provided, source will be treated as null.

Returns a 204 on success, 400 when not upshelled, 401 when unauthorized.

## Webhook Events

UpNut takes stock of what's going on and is able to poll a webhook when something changes.

Current supported events:

- PING (type: String = "PING", time: DateTime)
- NEW_LIBRARY_CHAPTERS (type: String = "NEW_LIBRARY_CHAPTERS", chapters: List<LibraryChapter>)
- LIBRARY_CHAPTERS_REDACTED (type: String = "LIBRARY_CHAPTERS_REDACTED", chapters: List<LibraryChapter>)
- LIBRARY_CHAPTERS_UNREDACTED (type: String = "LIBRARY_CHAPTERS_UNREDACTED", chapters: List<LibraryChapter>)

- THRESHOLD_PASSED_NUTS (type: String = "THRESHOLD_PASSED_NUTS", threshold: Int, time: Long, event: BlaseballEvent)
- THRESHOLD_PASSED_SCALES (type: String = "THRESHOLD_PASSED_SCALES", threshold: Int, time: Long, event: BlaseballEvent)

LibraryChapter (bookName: String, chapterUUID: UUID, chapterName: String | null, chapterNameRedacted: String | null, isRedacted: Boolean)

Events are resent every 30m if the server doesn't respond successfully (2xx range).

Events are sent to a webhook with the data, and a signature attached in the header, X-UpNut-Signature, calculated with HmacSHA256.

### Sign me up!
Do you have a project you'd like to integrate with UpNuts? Get in touch with me on Discord at UnderMybrella#1084, and I can set you up!

For providers, you'll need to give me an RSA public key, as well as a provider UUID (or I can generate one randomly for you). Sources are unique to each provider.

For webhooks, I just need a webhook URL, and I can generate a secret key, or you can provide one. I also just need the types of events you want to receive.
