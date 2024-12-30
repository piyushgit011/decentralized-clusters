# main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from datetime import datetime, timedelta
import jwt
import sqlite3
from passlib.context import CryptContext

# SQLite setup
DATABASE_NAME = "providers.db"
conn = sqlite3.connect(DATABASE_NAME)
cursor = conn.cursor()

# Create providers table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    hashed_password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# FastAPI setup
app = FastAPI()

# JWT settings
SECRET_KEY = "your-secret-key"  # Change this to a secure random key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 setup
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Pydantic models
class ProviderCreate(BaseModel):
    username: str
    password: str

class Provider(BaseModel):
    id: int
    username: str
    created_at: datetime

class Token(BaseModel):
    access_token: str
    token_type: str

# Helper functions
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_provider(username: str):
    cursor.execute("SELECT * FROM providers WHERE username=?", (username,))
    provider = cursor.fetchone()
    if provider:
        return Provider(id=provider[0], username=provider[1], created_at=datetime.fromisoformat(provider[3]))
    return None

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# API routes
@app.post("/register", response_model=Provider)
async def register_provider(provider: ProviderCreate):
    cursor.execute("SELECT * FROM providers WHERE username=?", (provider.username,))
    if cursor.fetchone():
        raise HTTPException(status_code=400, detail="Username already registered")
    
    hashed_password = pwd_context.hash(provider.password)
    cursor.execute("INSERT INTO providers (username, hashed_password) VALUES (?, ?)", 
                   (provider.username, hashed_password))
    conn.commit()
    
    cursor.execute("SELECT * FROM providers WHERE username=?", (provider.username,))
    new_provider = cursor.fetchone()
    return Provider(id=new_provider[0], username=new_provider[1], created_at=datetime.fromisoformat(new_provider[3]))

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    cursor.execute("SELECT * FROM providers WHERE username=?", (form_data.username,))
    provider = cursor.fetchone()
    if not provider or not verify_password(form_data.password, provider[2]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": provider[1]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/providers/me", response_model=Provider)
async def read_providers_me(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    provider = get_provider(username)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    return provider

@app.on_event("shutdown")
async def shutdown():
    conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# File ends here