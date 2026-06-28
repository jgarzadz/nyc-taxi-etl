import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("etl.extractors.taxi")

NYC_TLC_BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)


class TaxiExtractor:
    """
    Extrae datos de viajes de taxi amarillo de NYC.
    Solo descarga y lee datos crudos — no transforma nada.
    """

    COLUMNS_NEEDED = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
    ]

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"TaxiExtractor inicializado. Directorio: {self.data_dir}")


    def download(self, year_month: str) -> Path:
        """
        Descarga el archivo Parquet si no existe localmente.
        Retorna la ruta local del archivo.
        """
        filename = f"yellow_tripdata_{year_month}.parquet"
        local_path = self.data_dir / filename

        if local_path.exists():
            logger.info(f"Archivo ya existe localmente: {filename}")
            return local_path

        url = f"{NYC_TLC_BASE_URL}/yellow_tripdata_{year_month}.parquet"
        logger.info(f"Descargando: {url}")

        try:
            response = requests.get(url, timeout=120, stream=True)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_mb = local_path.stat().st_size / (1024 * 1024)
            logger.info(f"Descarga completa: {filename} ({size_mb:.1f} MB)")
            return local_path

        except requests.RequestException as e:
            logger.error(f"Error al descargar {url}: {e}")
            if local_path.exists():
                local_path.unlink()
            raise
    
    def extract(self, file_path: Path) -> pd.DataFrame:
        """
        Lee el archivo Parquet y retorna un DataFrame crudo.
        Solo lee las columnas necesarias para eficiencia.
        """
        logger.info(f"Leyendo archivo: {file_path.name}")

        df = pd.read_parquet(
            file_path,
            columns=self.COLUMNS_NEEDED,
            engine="pyarrow",
        )

        logger.info(
            f"Extraídas {len(df):,} filas | "
            f"Columnas: {len(df.columns)} | "
            f"Memoria: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB"
        )
        return df