import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

load_dotenv()

_host = os.getenv("POSTGRES_HOST", "/tmp")
_port = os.getenv("POSTGRES_PORT", "5432")
_db   = os.getenv("POSTGRES_DB", "medical_warehouse")
_user = os.getenv("POSTGRES_USER", "selam")
_pw   = os.getenv("POSTGRES_PASSWORD", "")

# Build connection URL — support both Unix socket (host starts with /)
# and standard TCP connections.
if _host.startswith("/"):
    DATABASE_URL = f"postgresql+psycopg2://{_user}@/{_db}?host={_host}"
else:
    DATABASE_URL = f"postgresql+psycopg2://{_user}:{_pw}@{_host}:{_port}/{_db}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
