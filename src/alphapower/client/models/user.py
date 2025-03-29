import json


class Authentication_User:
    def __init__(self, id):
        self.id = id


class Authentication_Token:
    def __init__(self, expiry):
        self.expiry = expiry


class Authentication:
    def __init__(self, user, token, permissions):
        self.user = Authentication_User(**user)
        self.token = Authentication_Token(**token)
        self.permissions = permissions

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)
