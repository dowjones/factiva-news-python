"""Define common errors in the library."""
UNEXPECTED_HTTP_ERROR = RuntimeError('API Request returned an unexpected HTTP status')
UNDEFINED_STREAM_ID_ERROR = ValueError('Undefined stream id')
INVALID_SUBSCRIPTION_ID_ERROR = ValueError('Invalid subscription id')
UNDEFINED_SUBSCRIPTION_ERROR = ValueError('No subscription specified. You must specify the subscription ID')
