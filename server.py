# FastAPI Backend (backend.py)
from fastapi import FastAPI, Query, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import logging

load_dotenv()

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Connection
DATABASE_URL = os.getenv("DATABASE_URL").strip()
schema_name = os.getenv("SCHEMA_NAME").strip()

print("DATABASE_URL: ", DATABASE_URL)
print("SCHEMA_NAME:", schema_name , " type:", type(schema_name))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# Scoped session to manage per-request database sessions
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

def get_db():
    db = SessionLocal()
    try:
        yield db  # Return the session
    finally:
        db.close()  # Ensure session is closed after request

# Common response helpers
def success_response(message: str, data: dict = None):
    return {
        "success": True,
        "message": message,
        "data": data if data else {}
    }

def error_response(message: str, error: str = None):
    HTTPException(status_code=400, detail=message)

@app.get("/user_tracks/")
def get_user_tracks(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    try:
        offset = (page - 1) * page_size
        table_name = "user_tracks_history_formatted"

        # Fetch paginated user tracks
        query = text(f"SELECT * FROM {schema_name}.{table_name} ORDER BY played_at DESC LIMIT :limit OFFSET :offset")
        result = db.execute(query, {"limit": page_size, "offset": offset}).fetchall()
        
        # Fetch total record count
        count_query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        total_records = db.execute(count_query).scalar()

        data = [row._mapping for row in result]
        
        return success_response(
            "User tracks retrieved successfully",
            {"history": data, "total": total_records}
        )

    except SQLAlchemyError as e:
        db.rollback()  # Rollback transaction if an error occurs
        raise HTTPException(status_code=500, detail="Database error")

    except Exception as e:
        logging.error(f"Error retrieving user tracks: {e}")
        return error_response("Failed to retrieve user tracks", str(e))

@app.get("/tracks/")
def get_tracks(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    try:
        offset = (page - 1) * page_size
        table_name = "tracks_formatted"
        
        query = text(f"""
            SELECT * FROM {schema_name}.{table_name} 
            LIMIT :limit OFFSET :offset
        """)
        result = db.execute(query, {"limit": page_size, "offset": offset}).fetchall()

        count_query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        total_records = db.execute(count_query).scalar()

        data = [row._mapping for row in result]
        
        return success_response(
            "Tracks retrieved successfully",
            {"track": data, "total": total_records}
        )

    except SQLAlchemyError as e:
        db.rollback()  # Rollback transaction if an error occurs
        raise HTTPException(status_code=500, detail="Database error")

    except Exception as e:
        logging.error(f"Error retrieving tracks: {e}")
        return error_response("Failed to retrieve tracks", str(e))

@app.get("/artists/")
def get_artists(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    try:
        offset = (page - 1) * page_size
        table_name = "artists_formatted"
        
        query = text(f"""
            SELECT * FROM {schema_name}.{table_name} 
            LIMIT :limit OFFSET :offset
        """)
        result = db.execute(query, {"limit": page_size, "offset": offset}).fetchall()

        count_query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        total_records = db.execute(count_query).scalar()

        data = [row._mapping for row in result]
        
        return success_response(
            "Artists retrieved successfully",
            {"artist": data, "total": total_records}
        )

    except SQLAlchemyError as e:
        db.rollback()  # Rollback transaction if an error occurs
        raise HTTPException(status_code=500, detail="Database error")

    except Exception as e:
        logging.error(f"Error retrieving artists: {e}")
        return error_response("Failed to retrieve artists", str(e))

@app.get("/albums/")
def get_albums(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)):
    try:
        offset = (page - 1) * page_size
        table_name = "albums_formatted"
        
        query = text(f"""
            SELECT * FROM {schema_name}.{table_name} 
            ORDER BY release_date DESC 
            LIMIT :limit OFFSET :offset
        """)
        result = db.execute(query, {"limit": page_size, "offset": offset}).fetchall()

        count_query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        total_records = db.execute(count_query).scalar()

        data = [row._mapping for row in result]
            
        return success_response(
            "Albums retrieved successfully",
            {"album": data, "total": total_records}
        )

    except SQLAlchemyError as e:
        db.rollback()  # Rollback transaction if an error occurs
        raise HTTPException(status_code=500, detail="Database error")

    except Exception as e:
        logging.error(f"Error retrieving albums: {e}")
        return error_response("Failed to retrieve albums", str(e))

# Global exception handler
@app.exception_handler(Exception)
def global_exception_handler(request: Request, exc: Exception):
    logging.exception(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"}
    )

# Optional HTTPException handler
@app.exception_handler(HTTPException)
def http_exception_handler(request: Request, exc: HTTPException):
    logging.warning(f"HTTP exception: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )