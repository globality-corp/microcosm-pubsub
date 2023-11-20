sample_event = {
    'level': 'error',
    'exception': {
        'values': [
            {
                'module': None, 'type': 'TypeError', 'value': 'Testing stuff', 'mechanism': None,
                'stacktrace': {
                    'frames': [
                        {
                            'filename': 'microcosm_pubsub/result.py',
                            'abs_path': '/Users/rob/dev/microcosm-pubsub/microcosm_pubsub/result.py',
                            'function': 'invoke',
                            'module': 'microcosm_pubsub.result', 'lineno': 85,
                            'pre_context': [
                                '    retry_timeout_seconds: Optional[int] = None', '', '    @classmethod',
                                '    def invoke(cls, handler, message: SQSMessage):', '        try:'
                            ],
                            'context_line': '            success = handler(message.content)',
                            'post_context': [
                                '            return cls.from_result(message, bool(success))',
                                '        except Exception as error:',
                                '            return cls.from_error(message, error)',
                                '',
                                '    @classmethod'
                            ],
                            'vars': {
                                'cls': "<class 'microcosm_pubsub.result.MessageHandlingResult'>",
                                'handler': '<function context_logger.<locals>.wrapped at 0x109422ef0>',
                                'message': '<microcosm_pubsub.message.SQSMessage object at 0x10952bcd0>'
                            },
                            'in_app': True},
                        {
                            'filename': 'test/daemon/test_daemon/handlers/test_handler.py',
                            'abs_path': '/Users/rob/dev/test/daemon/test_daemon/handlers/test_handler.py',
                            'function': 'do_something',
                            'module': 'test.daemon.test_daemon.handlers.test_handler', 'lineno': 50,
                            'pre_context': [
                                '    def resource_type(self):',
                                '        return self.test.get_model("TestHandler")',
                                '', '    @extracts("something")',
                            ],
                            'context_line': '        raise TypeError("Testing stuff")',
                            'post_context': ['    @extracts("something")'],
                            'vars': {
                                'self':
                                    '<test.daemon.test_daemon.handlers.test_handler.TestHandler object at 0x10953cbd0>',
                                'something_id': "'1f40066c-f457-41b3-aa4c-72cdac5146e4'",
                                'project_description': "'this is some secret info'",
                                'other_id': "'70375dff-2d46-40c4-a1d1-f5d49a25698d'"
                            }, 'in_app': True
                        }
                    ]
                }
            }
        ]
    },
    'event_id': 'c25a874a6d964c8b832e00c10009d9bc', 'timestamp': '2020-05-16T11:01:35.987342Z',
    'breadcrumbs': [
        {'ty': 'log', 'level': 'info', 'category': 'something', 'message': 'Starting daemon test',
         'timestamp': '2020-05-16T11:01:35.935538Z', 'data': {}, 'type': 'default'},
        {'ty': 'log', 'level': 'warning', 'category': 'TestHandler',
         'message': 'Result for media type: application/vnd.globality.pubsub._.created.do_something was : FAILED ',
         'timestamp': '2020-05-16T11:01:35.983545Z',
         'data': {'media_type': 'application/vnd.globality.pubsub._.created.do_something',
                  'message_id': 'message-id-b7fa5993-a966-4390-a6b1-ed9eb5026134', 'x-request-ttl': '31',
                  'uri': 'http://localhost:5452/api/v2/message/6dee4da6-8af1-4636-93b6-7770bc6990bc',
                  'handler': 'Handler Test', 'elapsed_time': 47.17707633972168}, 'type': 'default'}
    ],
    'tags': {'x-request-id': None, 'message-id': 'message-id-b7fa5993-a966-4390-a6b1-ed9eb5026134', 'media-type': None},
    'contexts': {'runtime': {'name': 'CPython', 'version': '3.7.4',
                             'build': ''}},
    'modules': {'microcosm-pubsub': '2.17.0'},
    'extra': {'sys.argv': []}, 'environment': 'localhost',
    'server_name': 'some_daemon',
    'sdk': {'name': 'sentry.python', 'version': '0.14.4',
            'packages': [{'name': 'pypi:sentry-sdk', 'version': '0.14.4'}],
            'integrations': []},
    'platform': 'python'
}
