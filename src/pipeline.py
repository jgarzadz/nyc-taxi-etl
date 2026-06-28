import logging
import sys
from pathlib import Path

from src.utils.config import load_config
from src.extractors.taxi_extractor import TaxiExtractor


def setup_logging() -> logging.Logger:
    """Configura el sistema de logging para todo el pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("etl.pipeline")

def run_pipeline() -> None:
    """
    Orquesta el pipeline ETL completo.
    Flujo: configuración → extracción → (transformación → carga, próxima sesión)
    """
    logger = setup_logging()

    logger.info("=" * 50)
    logger.info("Iniciando pipeline: NYC Taxi ETL")
    logger.info("=" * 50)

    try:
        # ── CONFIGURACIÓN ─────────────────────────────
        logger.info("Cargando configuración...")
        config = load_config()
        logger.info(f"Base de datos: {config.url_safe}")

        # ── EXTRACCIÓN ────────────────────────────────
        logger.info("Iniciando extracción...")
        extractor = TaxiExtractor(
            data_dir=Path("/app/data")
        )

        file_path = extractor.download(year_month=config.taxi_year_month)
        df = extractor.extract(file_path=file_path)

        logger.info(f"Extracción completa: {len(df):,} filas")
        logger.info("Transformación y carga: próxima sesión")

    except EnvironmentError as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error inesperado: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()