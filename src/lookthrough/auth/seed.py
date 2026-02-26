"""Seed script to create default admin user.

Run with: python -m src.lookthrough.auth.seed
"""

import uuid

from src.lookthrough.db.engine import get_session_context, init_db
from src.lookthrough.db.models import User

from .utils import hash_password

DEFAULT_ADMIN_EMAIL = "admin@lookthrough.com"
DEFAULT_ADMIN_PASSWORD = "admin123"
DEFAULT_ADMIN_NAME = "Administrator"


def seed_admin_user() -> None:
    """Create default admin user if it doesn't exist."""
    # Ensure tables exist
    init_db()

    with get_session_context() as session:
        existing_user = (
            session.query(User).filter(User.email == DEFAULT_ADMIN_EMAIL).first()
        )

        if existing_user:
            print(f"Admin user already exists: {DEFAULT_ADMIN_EMAIL}")
            return

        admin_user = User(
            id=str(uuid.uuid4()),
            email=DEFAULT_ADMIN_EMAIL,
            hashed_password=hash_password(DEFAULT_ADMIN_PASSWORD),
            full_name=DEFAULT_ADMIN_NAME,
            is_active=True,
        )

        session.add(admin_user)
        print(f"Created admin user: {DEFAULT_ADMIN_EMAIL}")


if __name__ == "__main__":
    seed_admin_user()
