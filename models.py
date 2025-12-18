from sqlalchemy import Column, Integer, String, Text, ARRAY, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import create_engine
import os

Base = declarative_base()

class Business(Base):
    __tablename__ = 'businesses'

    id = Column(Integer, primary_key=True, autoincrement=True)
    place_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    city = Column(String, nullable=True)
    full_address = Column(Text, nullable=True)
    category = Column(String, nullable=True)
    website = Column(String, nullable=True)
    maps_url = Column(String, nullable=True)
    keywords_found = Column(ARRAY(String), nullable=True)

    # Deduplication: ON CONFLICT (place_id) DO UPDATE is handled by the insert logic,
    # but the unique constraint here ensures schema integrity.
    __table_args__ = (
        UniqueConstraint('place_id', name='uq_place_id'),
    )

    def to_dict(self):
        return {
            "place_id": self.place_id,
            "name": self.name,
            "city": self.city,
            "full_address": self.full_address,
            "category": self.category,
            "website": self.website,
            "maps_url": self.maps_url,
            "keywords_found": self.keywords_found,
        }

# Database URL construction
# We need both Sync and Async engines for different parts of the app
# (Sync for export/seed initialization, Async for workers)

DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "scraper_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Sync Engine
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Async Engine
async_engine = create_async_engine(ASYNC_DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

def init_db():
    """Synchronous database initialization."""
    Base.metadata.create_all(bind=engine)
