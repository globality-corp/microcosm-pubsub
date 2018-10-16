"""
Message handling result.

"""
from dataclasses import dataclass
from enum import Enum, unique
from logging import DEBUG, INFO, WARNING
from sys import exc_info
from typing import Any, Dict, Tuple

from microcosm_pubsub.errors import IgnoreMessage, Nack, SkipMessage, TTLExpired


@dataclass
class MessageHandlingResultTypeInfo:
    name: str
    level: int
    exc_info: bool = False


@unique
class MessageHandlingResultType(Enum):
    # Messaging handling aborted due to too many attempts.
    EXPIRED = MessageHandlingResultTypeInfo(name="EXPIRED", level=WARNING)

    # Messaging handling failed.
    FAILED = MessageHandlingResultTypeInfo(name="FAILED", level=WARNING, exc_info=True)

    # Message was not handled.
    IGNORED = MessageHandlingResultTypeInfo(name="IGNORED", level=DEBUG)

    # Messaging handling was intentionally retried.
    RETRIED = MessageHandlingResultTypeInfo(name="RETRIED", level=INFO)

    # Message handling succeeded.
    SUCCEEDED = MessageHandlingResultTypeInfo(name="SUCCEEDED", level=INFO)

    # Message handling was skipped.
    # ("Upon closer inspection these are loafers")
    SKIPPED = MessageHandlingResultTypeInfo(name="SKIPPED", level=INFO)

    def __str__(self):
        return self.name

    @property
    def level(self):
        return self.value.level

    @property
    def exc_info(self):
        return self.value.exc_info


@dataclass
class MessageHandlingResult:
    exc_info: Tuple[Any, Any, Any]
    extra: Dict[str, str]
    media_type: str
    result: MessageHandlingResultType
    elapsed_time: float = None

    @classmethod
    def invoke(cls, func, message, **kwargs):
        exc_info_ = None
        extra = dict()
        try:
            success = func(message=message, **kwargs)
            if success:
                result = MessageHandlingResultType.SUCCEEDED
            else:
                result = MessageHandlingResultType.SKIPPED
            message.ack()
        except IgnoreMessage as error:
            extra = error.extra
            result = MessageHandlingResultType.IGNORED
            message.ack()
        except SkipMessage as error:
            extra = error.extra
            result = MessageHandlingResultType.SKIPPED
            message.ack()
        except TTLExpired as error:
            extra = error.extra
            result = MessageHandlingResultType.EXPIRED
            message.ack()
        except Nack as nack:
            result = MessageHandlingResultType.RETRIED
            message.nack(nack.visibility_timeout_seconds)
        except Exception as error:
            exc_info_ = exc_info()
            result = MessageHandlingResultType.FAILED
            message.nack()

        return cls(
            exc_info=exc_info_,
            extra=extra,
            media_type=message.media_type,
            result=result,
        )

    def log(self, logger, opaque):
        opaque.update(self.extra)

        message = f"Result for media type: {self.media_type} was : {self.result} "

        logger.log(
            self.result.level,
            message,
            exc_info=self.exc_info,
            extra=opaque.as_dict(),
        )
