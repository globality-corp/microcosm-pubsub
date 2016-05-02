# Release Change Log

Update this file when creating new releases, with most recent releases first.

## Version 0.8.0

 -  Relaxes configuration defaults/assumptions for consumer daemons so that
    it is easier to provide default configurations that just work.

## Version 0.7.0

 -  Adds `UUIDField` that always deserializes to string, ensuring that UUIDs
    are JSON serializable when published as message content.
