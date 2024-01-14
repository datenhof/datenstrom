import requests
import os
import jwt
import re

from datetime import timedelta
from typing import Any
from jwt import PyJWKClient

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent



class AuthenticationEnrichment(BaseEnrichment):
    def __init__(self, config: Any) -> None:
        super().__init__(config=config)
        self.token_field = self.config.get("authentication_sub_field", "sub")
        self.aud = self.config.get("authentication_aud")
        self.iss_jwk_urls = self.config.get("authentication_iss_jwk_urls", {})
        self.public_key = self.config.get("authentication_public_key")
        self.jwk_clients = {}

    def get_jwk_client(self, iss: str) -> PyJWKClient:
        if iss in self.jwk_clients:
            return self.jwk_clients[iss]

        for k, v in self.iss_jwk_urls.items():
            if iss in k:
                self.jwk_clients[iss] = PyJWKClient(v)
                return self.jwk_clients[iss]
        
        default_url = "{iss}/.well-known/jwks.json".format(iss=iss)
        print(f"Could not find jwk url for issuer: {iss}")
        print(f"Using default url: {default_url}")
        self.jwk_clients[iss] = PyJWKClient(default_url)
        return self.jwk_clients[iss]

    def get_public_key(self, token: str):
        if self.public_key is not None:
            return self.public_key
        try:
            infos = jwt.decode(token, options={"verify_signature": False})
        except jwt.exceptions.InvalidTokenError as e:
            raise ValueError(f"Invalid jwt (RS256) token: {e}")
        iss = infos["iss"]
        jwk_client = self.get_jwk_client(iss)
        jwk = jwk_client.get_signing_key_from_jwt(token)
        return jwk.key

    def decode_token(self, token: str):
        public_key = self.get_public_key(token)
        try:
            info = jwt.decode(token, public_key, algorithms=["RS256"], leeway=timedelta(minutes=60))
        except jwt.exceptions.InvalidTokenError as e:
            raise ValueError(f"Invalid jwt (RS256) token: {e}")
        if self.aud:
            if info["aud"] != self.aud:
                raise ValueError("Invalid aud in jwt token")
        if self.token_field not in info:
            raise ValueError(f"Did not find {self.token_field} in jwt token")
        return info

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        # get auth header
        collector_payload = event.raw_event
        headers = collector_payload.get_headers_dict()
        if "authorization" not in headers:
            return
        auth_header = headers["authorization"]
        # check if bearer token
        if not auth_header.lower().startswith("bearer "):
            return
        # try to decode token
        token = auth_header.split(" ")[1]
        try:
            token_claims = self.decode_token(token)
        except ValueError as e:
            print(f"Error in Auth Enrichment: {e}")
            return
        # get the field
        auth = token_claims[self.token_field]
        # set the field
        event.set_value("collector_auth", auth)
