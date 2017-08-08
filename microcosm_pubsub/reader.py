"""
Implement SQS message reading from other sources.

"""
from json import loads

from microcosm_daemon.error_policy import FatalError


class SQSFileReader(object):
    """
    Read message data from a file.

    """
    def __init__(self, path):
        self.path = path
        self.iter_ = iter(open(self.path))

    def receive_message(self, MaxNumberOfMessages, **kwargs):
        limit = MaxNumberOfMessages

        messages = []
        for _ in range(limit):
            try:
                message = next(self.iter_)
            except StopIteration:
                break
            else:
                messages.append(loads(message))

        if not messages:
            raise FatalError("No more messages to replay")
        return dict(Messages=messages)

    def delete_message(self, *args, **kwargs):
        pass

    def change_message_visibility(self, *args, **kwargs):
        pass
