import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    taxi_year_month: str

    @property
    def url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def url_safe(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:***"
            f"@{self.host}:{self.port}/{self.database}"
        )
    

def load_config() -> DatabaseConfig:
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_PASSWORD",
        "POSTGRES_USER",
        "POSTGRES_DB",
        "TAXI_YEAR_MONTH",
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(
            f"Variables de entorno faltantes: {missing}\n"
            f"Copia .env.example como .env y completa los valores."
        )

    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        taxi_year_month=os.getenv("TAXI_YEAR_MONTH"),
    )