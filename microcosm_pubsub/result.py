"""
Message handling result.

"""
from dataclasses import dataclass, field
from enum import Enum, unique
from logging import DEBUG, INFO, WARNING
from sys import exc_info
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

from microcosm_pubsub.errors import (
    IgnoreMessage,
    Nack,
    SkipMessage,
    TTLExpired,
)
from microcosm_pubsub.message import SQSMessage


@dataclass
class MessageHandlingResultTypeInfo:
    name: str
    level: int
    retry: bool = False
    exc_info: bool = False


@unique
class MessageHandlingResultType(Enum):
    # Messaging handling aborted due to too many attempts.
    EXPIRED = MessageHandlingResultTypeInfo(name="EXPIRED", level=WARNING)

    # Messaging handling failed.
    FAILED = MessageHandlingResultTypeInfo(name="FAILED", level=WARNING, exc_info=True, retry=True)

    # Message was not handled.
    IGNORED = MessageHandlingResultTypeInfo(name="IGNORED", level=DEBUG)

    # Messaging handling was intentionally retried.
    RETRIED = MessageHandlingResultTypeInfo(name="RETRIED", level=INFO, retry=True)

    # Message handling succeeded.
    SUCCEEDED = MessageHandlingResultTypeInfo(name="SUCCEEDED", level=INFO)

    # Message handling was skipped.
    # ("Upon closer inspection these are loafers")
    SKIPPED = MessageHandlingResultTypeInfo(name="SKIPPED", level=INFO)

    def __str__(self):
        return self.name

    @property
    def exc_info(self):
        return self.value.exc_info

    @property
    def level(self):
        return self.value.level

    @property
    def retry(self):
        return self.value.retry


@dataclass
class MessageHandlingResult:
    media_type: str
    result: MessageHandlingResultType
    exc_info: Optional[Tuple[Any, Any, Any]] = None
    extra: Dict[str, str] = field(default_factory=dict)
    elapsed_time: Optional[float] = None
    retry_timeout_seconds: Optional[int] = None

    @classmethod
    def invoke(cls, handler, message: SQSMessage):
        try:
            success = handler(message.content)
            return cls.from_result(message, bool(success))
        except Exception as error:
            return cls.from_error(message, error)

    @classmethod
    def from_result(cls, message: SQSMessage, success: bool):
        if success:
            result = MessageHandlingResultType.SUCCEEDED
        else:
            result = MessageHandlingResultType.SKIPPED

        return cls(
            media_type=message.media_type,
            result=result,
        )

    @classmethod
    def from_error(cls, message: SQSMessage, error: Exception, **kwargs):
        if isinstance(error, IgnoreMessage):
            return cls(
                extra=error.extra,
                media_type=message.media_type,
                result=MessageHandlingResultType.IGNORED,
            )

        if isinstance(error, SkipMessage):
            return cls(
                extra=dict(
                    reason=str(error),
                    **error.extra
                ),
                media_type=message.media_type,
                result=MessageHandlingResultType.SKIPPED,
            )

        if isinstance(error, TTLExpired):
            return cls(
                extra=error.extra,
                media_type=message.media_type,
                result=MessageHandlingResultType.EXPIRED,
            )

        if isinstance(error, Nack):
            return cls(
                media_type=message.media_type,
                result=MessageHandlingResultType.RETRIED,
                retry_timeout_seconds=error.visibility_timeout_seconds,
            )

        return cls(
            exc_info=exc_info(),
            media_type=message.media_type,
            result=MessageHandlingResultType.FAILED,
        )

    def log(self, logger, opaque):
        """
        Log this result.

        """
        entry = f"Result for media type: {self.media_type} was : {self.result} "
        logger.log(
            self.result.level,
            entry,
            exc_info=self.exc_info,
            extra=dict(**self.extra, **opaque.as_dict()),
        )

    def resolve(self, message):
        if self.result.retry:
            message.nack(self.retry_timeout_seconds)
        else:
            message.ack()
        return self
