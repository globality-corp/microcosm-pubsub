"""
Message handling result.

"""
from dataclasses import dataclass
from enum import Enum, unique

from microcosm_pubsub.errors import IgnoreMessage, Nack, SkipMessage, TTLExpired


@unique
class MessageHandlingResultType(Enum):
    # Messaging handling aborted due to too many attempts.
    EXPIRED = "EXPIRED"

    # Messaging handling failed.
    FAILED = "FAILED"

    # Message was not handled.
    IGNORED = "IGNORED"

    # Messaging handling was intentionally retried.
    RETRIED = "RETRIED"

    # Message handling succeeded.
    SUCCEEDED = "SUCCEEDED"

    # Message handling was skipped.
    # ("Upon closer inspection these are loafers")
    SKIPPED = "SKIPPED"

    def __str__(self):
        return self.name


@dataclass
class MessageHandlingResult:
    elapsed_time: float
    media_type: str
    result: MessageHandlingResultType

    @classmethod
    def invoke(cls, func, message, opaque=None, **kwargs):
        try:
            success = func(message=message, **kwargs)
            if success:
                result = MessageHandlingResultType.SUCCEEDED
            else:
                result = MessageHandlingResultType.SKIPPED
            message.ack()
        except IgnoreMessage:
            result = MessageHandlingResultType.IGNORED
            message.ack()
        except SkipMessage:
            result = MessageHandlingResultType.SKIPPED
            message.ack()
        except TTLExpired:
            result = MessageHandlingResultType.EXPIRED
            message.ack()
        except Nack as nack:
            result = MessageHandlingResultType.RETRIED
            message.nack(nack.visibility_timeout_seconds)
        except Exception as error:
            result = MessageHandlingResultType.FAILED
            message.nack()

        return cls(
            # XXX will not yet work because `invoke` is called outside of timing loop
            elapsed_time=opaque.as_dict().get("elapsed_time") if opaque else None,
            media_type=message.media_type,
            result=result,
        )
