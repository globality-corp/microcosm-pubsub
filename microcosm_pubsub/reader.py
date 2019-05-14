"""
Implement SQS message reading from other sources.

"""
from json import loads
from sys import stdin

from microcosm_daemon.error_policy import ExitError


class SQSFileReader:
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
            raise ExitError("No more messages to replay")
        return dict(Messages=messages)

    def delete_message(self, *args, **kwargs):
        pass

    def change_message_visibility(self, *args, **kwargs):
        pass


class SQSStdInReader:
    """
    Read message data from stdin.

    """
    def receive_message(self, MaxNumberOfMessages, **kwargs):
        limit = MaxNumberOfMessages

        messages = []
        for _ in range(limit):
            message = stdin.readline()
            if message == "":
                break
            messages.append(loads(message))

        if not messages:
            raise ExitError("No more messages to replay")
        return dict(Messages=messages)

    def delete_message(self, *args, **kwargs):
        pass

    def change_message_visibility(self, *args, **kwargs):
        pass
