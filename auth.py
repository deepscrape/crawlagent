import os
from typing import Callable, Dict

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from firestore import auth

security = HTTPBearer()


# Use an Firebase API key (32 characters long)
"""  api_key  """

def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> Dict:
    """Verify the bearer token against environment variable or Firebase.

    Simple JWT verification. Expects header like "Bearer <token>"
    Returns decoded payload or raises HTTPException.
    """
    try:
        if credentials is None or not credentials.credentials:
            raise HTTPException(status_code=401, detail="missing authorization")

        if credentials.scheme is None or credentials.scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="invalid authorization header")

        token = credentials.credentials


        # Fallback to Firebase token verification
        decoded_token = auth.verify_id_token(token)
        request.state.user = decoded_token
        request.state.uid = decoded_token.get("uid")
        return decoded_token

    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from err


def get_token_dependency(config: Dict) -> Callable:
    """Return the token dependency if JWT is enabled, else a function that returns None."""

    if config is not None and config.get("security", {}).get("jwt_enabled", False):
        return verify_token
    else:
        return lambda: None