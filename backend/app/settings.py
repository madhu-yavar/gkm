from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg2://gkm:gkm@localhost:5432/gkm"
    jwt_secret: str = "change-me"
    jwt_issuer: str = "gkm"
    access_token_expire_minutes: int = 720
    storage_dir: str = "./storage"

    seed_admin_email: str = "admin@example.com"
    seed_admin_password: str = "admin1234"


settings = Settings()
