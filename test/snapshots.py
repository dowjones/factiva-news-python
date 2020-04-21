from factiva import APIKeyUser

aku = APIKeyUser('abcd1234abcd1234abcd1234abcd1234')

assert aku.api_key == 'abcd1234abcd1234abcd1234abcd1234'
assert aku._account_endpoint == 'https://api.dowjones.com/alpha/accounts/abcd1234abcd1234abcd1234abcd1234'

ako = APIKeyUser('abcd1234abcd1234abcd1234abcd1234', False)

assert ako.api_key == 'abcd1234abcd1234abcd1234abcd1234'
assert ako._account_endpoint is None

# ake = APIKeyUser()   # Assert exceptions
# ake = APIKeyUser('abc')
