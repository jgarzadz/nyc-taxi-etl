import logging
import sys
from pathlib import Path

from src.utils.config import load_config
from src.extractors.taxi_extractor import TaxiExtractor
from src.transformers.taxi_transformer import TaxiTransformer
from src.loaders.postgres_loader import PostgresLoader

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
    Flujo: configuración → extracción → transformación → carga
    """
    logger = setup_logging()

    logger.info("=" * 50)
    logger.info("Iniciando pipeline: NYC Taxi ETL")
    logger.info("=" * 50)

    config = load_config()
    loader = PostgresLoader(database_url=config.url)
    run_id = loader.create_run(pipeline_name="nyc_taxi_etl")

    rows_extracted = 0
    rows_loaded = 0

    try:
        # ── CONFIGURACIÓN ─────────────────────────────
        logger.info(f"Base de datos: {config.url_safe}")
        logger.info(f"Período: {config.taxi_year_month}")

        # ── EXTRACCIÓN ────────────────────────────────
        logger.info("Fase 1: Extracción")
        extractor = TaxiExtractor(data_dir=Path("/app/data"))
        file_path = extractor.download(year_month=config.taxi_year_month)
        df_raw = extractor.extract(file_path=file_path)
        rows_extracted = len(df_raw)

        # ── TRANSFORMACIÓN ────────────────────────────
        logger.info("Fase 2: Transformación")
        transformer = TaxiTransformer()
        df_clean = transformer.transform(
            df=df_raw,
            source_file=file_path.name,
        )

        # ── CARGA ─────────────────────────────────────
        logger.info("Fase 3: Carga a PostgreSQL")
        rows_loaded = loader.load_taxi_trips(
            df=df_clean,
            run_id=run_id,
        )

        # ── LIMPIEZA ──────────────────────────────────
        logger.info("Limpiando archivo temporal...")
        file_path.unlink()
        logger.info(f"Archivo {file_path.name} eliminado")

        # ── ÉXITO ─────────────────────────────────────
        loader.finish_run(
            run_id=run_id,
            status="success",
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
        )

        logger.info("=" * 50)
        logger.info("Pipeline completado exitosamente")
        logger.info(f"  Extraídas : {rows_extracted:,} filas")
        logger.info(f"  Cargadas  : {rows_loaded:,} filas")
        logger.info(f"  Descartadas: {rows_extracted - rows_loaded:,} filas")
        logger.info("=" * 50)

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Pipeline FALLIDO: {error_msg}", exc_info=True)
        loader.finish_run(
            run_id=run_id,
            status="failed",
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            error_message=error_msg,
        )
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()