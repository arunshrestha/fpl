from db.postgres_client import get_engine
from sqlalchemy import text

def main():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT now()"))
        print(f"[OK] Connected, DB time: {result.scalar()}")

if __name__ == "__main__":
    main()
