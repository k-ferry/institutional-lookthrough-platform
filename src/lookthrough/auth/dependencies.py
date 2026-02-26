"""FastAPI dependencies for authentication."""

from fastapi import Cookie, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.lookthrough.db.engine import get_session
from src.lookthrough.db.models import User

from .utils import decode_access_token


def get_db() -> Session:
    """Get database session dependency."""
    session = get_session()
    try:
        yield session
    finally:
        session.close()


async def get_current_user(
    access_token: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> User:
    """Get the current authenticated user from JWT cookie.

    Args:
        access_token: JWT token from httpOnly cookie
        db: Database session

    Returns:
        Authenticated User object

    Raises:
        HTTPException: If token is missing, invalid, or user not found
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if access_token is None:
        raise credentials_exception

    payload = decode_access_token(access_token)
    if payload is None:
        raise credentials_exception

    user_id: str | None = payload.get("sub")
    if user_id is None:
        raise credentials_exception

    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise credentials_exception

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled",
        )

    return user
